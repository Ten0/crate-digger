package org.deepsymmetry.cratedigger;

import org.acplt.oncrpc.*;
import org.apiguardian.api.API;
import org.deepsymmetry.cratedigger.rpc.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * <p>Retrieves data files from a Pioneer player over NFS to enable use of track metadata even when four players
 * are using the same media.</p>
 *
 * <p>The primary purpose of this class is provided through the {@link #fetch(InetAddress, String, String, File)}
 * method.</p>
 *
 * <p>This is a singleton, so the single instance is obtained through the {@link #getInstance()} method.</p>
 */
@API(status = API.Status.STABLE)
public class FileFetcher {

    /**
     * The character set with which paths are sent to the NFS servers running on players.
     */
    @API(status = API.Status.STABLE)
    public final static Charset CHARSET = StandardCharsets.UTF_16LE;

    /**
     * The default number of bytes to read from the player in each request for file data. This is
     * {@link nfs#MAXDATA}, the largest read NFS v2 allows, which minimizes the number of round trips (each read
     * is a synchronous request/response, so throughput is dominated by their count). A read this large spans
     * several IP fragments, but lost fragments are recovered by the exponential retransmission we enable on the
     * client, so the larger size is a net win on the wired DJ Link network. Lower it with {@link #setReadSize(int)}
     * if you are fetching over a lossy link where retransmitting a whole large datagram is expensive.
     */
    @API(status = API.Status.STABLE)
    public final static int DEFAULT_READ_SIZE = nfs.MAXDATA;

    /**
     * The default number of file regions to read in parallel. Each read is synchronous (one request in flight per
     * client), so the transfer is round-trip-latency bound; reading disjoint regions over several clients at once
     * overlaps those round trips and multiplies throughput. The default of {@code 1} preserves the gentle, single
     * stream behavior; raise it with {@link #setConcurrency(int)} to speed up large transfers (a value of {@code 2}
     * already reaches the link's throughput ceiling on an XDJ-XZ). Note that values above {@code 1} open that many
     * simultaneous read streams to a player that may be performing live, so weigh the load.
     */
    @API(status = API.Status.STABLE)
    public static final int DEFAULT_CONCURRENCY = 1;

    /**
     * How long to wait for a response to our UDP RPC calls before retransmitting. The players respond within a
     * few milliseconds if they are going to at all.
     */
    @API(status = API.Status.STABLE)
    public static final int DEFAULT_RPC_RETRANSMIT_TIMEOUT = 250;

    /**
     * Below this many bytes per region we do not bother splitting a transfer across parallel clients, so small
     * files (the common case for metadata) keep using a single read stream regardless of {@link #concurrency}.
     */
    private static final int MIN_BYTES_PER_CONNECTION = 1 << 20;

    /**
     * The datagram buffer size for our NFS clients. It must hold a full read reply: up to {@link nfs#MAXDATA}
     * bytes of file data plus the RPC and NFS reply headers. Sizing it for the maximum read means a client stays
     * usable after {@link #setReadSize(int)} changes, and a reply for the largest read never overflows the buffer.
     */
    private static final int NFS_CLIENT_BUFFER_SIZE = nfs.MAXDATA + 1024;

    /**
     * Holds the singleton instance of this class.
     */
    private static final FileFetcher instance = new FileFetcher();

    /**
     * Look up the singleton instance of this class.
     *
     * @return the only instance that exists
     */
    @API(status = API.Status.STABLE)
    public static FileFetcher getInstance() {
        return instance;
    }

    /**
     * Make sure the only way to get an instance is to call {@link #getInstance()}.
     */
    private FileFetcher() {
        // Prevent instantiation.
    }

    /**
     * Check the number of bytes to read from the player in each request for file data. This is a trade-off
     * between reducing the number of requests and reducing IP fragmentation and expensive retransmissions
     * of already-sent fragments whenever one is lost.
     *
     * @return the current read size
     */
    @API(status = API.Status.STABLE)
    public int getReadSize() {
        return readSize;
    }

    /**
     * Set the number of bytes to read from the player in each request for file data. This is a trade-off
     * between reducing the number of requests and reducing IP fragmentation and expensive retransmissions
     * of already-sent fragments whenever one is lost. Changes do not affect operations already in progress.
     *
     * @param readSize the new read size, must be between 1024 and the largest value supported by NFS, inclusive
     * @throws IllegalArgumentException if {@code readSize} is less than 1024 or greater than {@link nfs#MAXDATA}
     */
    @API(status = API.Status.STABLE)
    public void setReadSize(int readSize) {
        if (readSize < 1024 || readSize > nfs.MAXDATA) {
            throw new IllegalArgumentException("readSize must be between 1024 and " + nfs.MAXDATA + ", inclusive.");
        }
        this.readSize = readSize;
    }

    /**
     * The number of bytes to read from the player in each request for file data. This is a trade-off
     * between reducing the number of requests and reducing IP fragmentation and expensive retransmissions
     * of already-sent fragments whenever one is lost.
     */
    private volatile int readSize = DEFAULT_READ_SIZE;

    /**
     * Check how many file regions are read in parallel during a fetch.
     *
     * @return the current concurrency
     */
    @API(status = API.Status.STABLE)
    public int getConcurrency() {
        return concurrency;
    }

    /**
     * Set how many file regions to read in parallel during a fetch. Because each read is a synchronous round trip,
     * reading disjoint regions over several clients at once overlaps those round trips and multiplies throughput on
     * large transfers; on an XDJ-XZ a value of {@code 2} already saturates the link. Values above {@code 1} open
     * that many simultaneous read streams to the player, so weigh the extra load on a unit that may be playing
     * live. Small files are never split (see {@code MIN_BYTES_PER_CONNECTION}). Changes do not affect operations
     * already in progress.
     *
     * @param concurrency the maximum number of parallel read streams per fetch, must be at least 1
     * @throws IllegalArgumentException if {@code concurrency} is less than 1
     */
    @API(status = API.Status.STABLE)
    public void setConcurrency(int concurrency) {
        if (concurrency < 1) {
            throw new IllegalArgumentException("concurrency must be at least 1.");
        }
        this.concurrency = concurrency;
    }

    /**
     * The maximum number of file regions to read in parallel during a fetch.
     */
    private volatile int concurrency = DEFAULT_CONCURRENCY;

    /**
     * Check how long to wait for a response to our UDP RPC calls before retransmitting. The players respond within a
     * few milliseconds if they are going to at all.
     *
     * @return the current retransmit timeout
     */
    @API(status = API.Status.STABLE)
    public int getRetransmitTimeout() {
        return retransmitTimeout;
    }

    /**
     * Set how long to wait for a response to our UDP RPC calls before retransmitting. The players respond within a
     * few milliseconds if they are going to at all. Changes do not affect operations already in progress.
     *
     * @param retransmitTimeout the new retransmit timeout, must be between 1 an 30000, inclusive
     */
    @API(status = API.Status.STABLE)
    public void setRetransmitTimeout(int retransmitTimeout) {
        if (retransmitTimeout < 1 || retransmitTimeout > 30000) {
            throw new IllegalArgumentException("retransmitTimeout must be between 1 and 30000, inclusive.");
        }
        this.retransmitTimeout = retransmitTimeout;
    }

    /**
     * How long to wait for a response to our UDP RPC calls before retransmitting. The players respond within a
     * few milliseconds if they are going to at all.
     */
    private volatile int retransmitTimeout = DEFAULT_RPC_RETRANSMIT_TIMEOUT;

    /**
     * Keeps track of the root filesystems of the known players, so we don't have to mount them every time we want a
     * file. Keys are the address of the player, values are a map from mount paths to the corresponding file handles.
     */
    private final Map<InetAddress, Map<String, FHandle>> mounts = new ConcurrentHashMap<>();

    /**
     * Mount a filesystem in preparation to retrieving files from it. Since NFS is a stateless protocol, and the
     * players don't even maintain a mount list, there is no need to unmount it later.
     *
     * @param player the address of the player from which we are going to retrieve a file
     * @param path the mount path of the filesystem from which the file is to be retrieved
     *
     * @return an indication of whether the mount was successful, and, if so, the root file handle of that filesystem
     *
     * @throws IOException if there is a problem talking to the player
     * @throws OncRpcException if there is a problem in the remote procedure call layer
     */
    private FHStatus mount (InetAddress player, String path) throws IOException, OncRpcException {
        OncRpcUdpClient client = (OncRpcUdpClient) OncRpcUdpClient.newOncRpcClient(player, mount.MOUNTPROG, mount.MOUNTVERS, OncRpcProtocols.ONCRPC_UDP);
        client.setRetransmissionTimeout(retransmitTimeout);
        client.setRetransmissionMode(OncRpcUdpRetransmissionMode.EXPONENTIAL);
        DirPath mountPath = new DirPath(path.getBytes(CHARSET));
        FHStatus result = new FHStatus();
        client.call(mount.MOUNTPROC_MNT_1, mountPath, result);
        client.close();
        return result;
    }

    /**
     * Looks up the root file handle for the filesystem exported by the specified player on the specified path. If we
     * have already mounted the filesystem, return the cached file handle. Otherwise, try to mount it, cache it, and
     * return it.
     *
     * @param player the address of the player from which we are going to retrieve a file
     * @param path the mount path of the filesystem from which the file is to be retrieved
     *
     * @return the file handle for the root of the filesystem exported at the specified path
     *
     * @throws IOException if there is a problem talking to the player
     * @throws OncRpcException if there is a problem in the remote procedure call layer
     */
    private FHandle findRoot(InetAddress player, String path) throws IOException, OncRpcException {
        Map<String, FHandle> playerMap = mounts.get(player);
        if (playerMap != null) {
            FHandle cached = playerMap.get(path);
            if (cached != null) {
                return cached;  // Found it, no need to mount again.
            }
        }

        // Not found in our cache, mount the filesystem from the player.
        FHStatus mountResult = mount(player, path);
        if (mountResult.status != 0) {
            throw new IOException("Unable to mount path \"" + path + "\" on player " + player.getHostAddress() +
                    ", mount command returned " + mountResult.status);
        }

        // Create a cache for the player if one does not yet exist.
        if (playerMap == null) {
            playerMap = new ConcurrentHashMap<>();
            mounts.put(player, playerMap);
        }

        // Put the new file handle in the player cache.
        playerMap.put(path, mountResult.directory);
        return mountResult.directory;
    }

    /**
     * Clear any cached mount points for the player with the specified address. This should be called when the
     * player drops off the network, in case a different player later appears on the same address, or if one of the
     * media slots in the player unmounts, because that file handle will no longer be valid. Also closes and clears
     * our cached NFS clients for the player.
     *
     * @param player the player that has disappeared or unmounted a filesystem
     */
    @API(status = API.Status.STABLE)
    public void removePlayer(InetAddress player) {
        mounts.remove(player);
        List<OncRpcUdpClient> pool = nfsClients.remove(player);
        if (pool != null) {
            for (OncRpcUdpClient client : pool) {
                try {
                    client.close();
                } catch (OncRpcException e) {
                    // Nothing useful to do; we are discarding the client anyway.
                }
            }
        }
    }

    /**
     * Holds a pool of NFS clients for talking to the player at the specified address, so we don't need to create a
     * new one each time we retrieve another file from the player. The pool grows on demand up to {@link #concurrency}
     * clients (each parallel read stream needs its own, since a client can only have one request in flight). Clients
     * are created the first time we need a file from a player, and closed when {@link #removePlayer(InetAddress)} is
     * called.
     */
    private final Map<InetAddress, List<OncRpcUdpClient>> nfsClients = new ConcurrentHashMap<>();

    /**
     * Find or create a single NFS client that can talk to a particular player (used for the mount-point lookups,
     * which are not parallelized).
     *
     * @param player the address of the player from which we are going to retrieve files
     *
     * @return an RPC client that can perform NFS calls on the specified player
     *
     * @throws IOException if there is a problem talking to the player
     * @throws OncRpcException if there is a problem in the remote procedure call layer
     */
    private OncRpcUdpClient getNfsClient(InetAddress player) throws OncRpcException, IOException {
        return getNfsClients(player, 1).get(0);
    }

    /**
     * Find or create at least {@code count} NFS clients that can talk to a particular player, so a fetch can read
     * that many file regions in parallel.
     *
     * @param player the address of the player from which we are going to retrieve files
     * @param count the number of clients the caller needs
     *
     * @return the pool of RPC clients (at least {@code count} of them) that can perform NFS calls on the player
     *
     * @throws IOException if there is a problem talking to the player
     * @throws OncRpcException if there is a problem in the remote procedure call layer
     */
    private synchronized List<OncRpcUdpClient> getNfsClients(InetAddress player, int count) throws OncRpcException, IOException {
        List<OncRpcUdpClient> pool = nfsClients.computeIfAbsent(player, p -> new ArrayList<>());
        while (pool.size() < count) {
            pool.add(createNfsClient(player));
        }
        for (OncRpcUdpClient client : pool) {
            client.setRetransmissionTimeout(retransmitTimeout);  // In case the value has changed since the last invocation.
        }
        return pool;
    }

    /**
     * Create a new NFS client for a player, with a datagram buffer large enough for our biggest reads and the
     * exponential retransmission the players need. The buffer can only be set at construction, and the constructor
     * needs the real NFS port (it does not consult the portmapper itself, unlike the protocol-based factory), so we
     * resolve the port first.
     *
     * @param player the address of the player to talk to
     *
     * @return a ready-to-use NFS client
     *
     * @throws IOException if there is a problem talking to the player
     * @throws OncRpcException if there is a problem in the remote procedure call layer
     */
    private OncRpcUdpClient createNfsClient(InetAddress player) throws OncRpcException, IOException {
        final int nfsPort;
        OncRpcPortmapClient portmap = new OncRpcPortmapClient(player, OncRpcProtocols.ONCRPC_UDP);
        try {
            nfsPort = portmap.getPort(nfs.NFS_PROGRAM, nfs.NFS_VERSION, OncRpcProtocols.ONCRPC_UDP);
        } finally {
            try {
                portmap.close();
            } catch (OncRpcException e) {
                // Ignore; we have what we needed from it.
            }
        }
        OncRpcUdpClient client = new OncRpcUdpClient(player, nfs.NFS_PROGRAM, nfs.NFS_VERSION, nfsPort, NFS_CLIENT_BUFFER_SIZE);
        client.setRetransmissionMode(OncRpcUdpRetransmissionMode.EXPONENTIAL);
        client.setRetransmissionTimeout(retransmitTimeout);
        return client;
    }

    /**
     * Try to find a file on a player, using our cached mount point if possible, or a new mount.
     *
     * @param player the address of the player from which we are going to retrieve a file
     * @param mountPath the mount path of the filesystem from which the file is to be retrieved
     * @param filePath the path to the file itself within the exported filesystem
     *
     * @return the result of looking up the last element in the path to the file
     *
     * @throws IOException if there is a problem talking to the player
     * @throws OncRpcException if there is a problem in the remote procedure call layer
     */
    private DirOpResBody find (InetAddress player, String mountPath, String filePath) throws IOException, OncRpcException {
        FHandle root = findRoot(player, mountPath);
        OncRpcUdpClient client = getNfsClient(player);

        // Iterate over the elements of the path to the file we want to find (the players can't handle multipart
        // path names themselves).
        String[] elements = filePath.split("/");
        FHandle fileHandle = root;
        DirOpRes result = null;
        for (String element : elements) {
            if (!element.isEmpty()) {
                DirOpArgs args = new DirOpArgs();
                args.dir = fileHandle;
                args.name = new Filename(element.getBytes(CHARSET));
                result = new DirOpRes();
                client.call(nfs.NFSPROC_LOOKUP_2, args, result);
                if (result.status != Stat.NFS_OK) {
                    String message = "Unable to find file \"" + filePath + "\", lookup of element \"" + element +
                            "\" returned status of " + result.status;
                    if (result.status == Stat.NFSERR_NOENT) {
                        throw new FileNotFoundException(message);
                    }
                    throw new IOException(message);
                }
                fileHandle = result.diropok.file;
            }
        }
        if (result == null) {
            throw new IllegalArgumentException("Must supply at least one non-empty mountPath element to look up.");
        }
        return result.diropok;
    }

    /**
     * Download a file from a player, storing it locally. Large files are read in parallel when {@link #concurrency}
     * is greater than one (see {@link #setConcurrency(int)}).
     *
     * @param player the address of the player from which we are to retrieve a file
     * @param mountPath the mount path of the filesystem from which the file is to be retrieved
     * @param sourcePath the path to the file itself within the exported filesystem
     * @param destination the local file to which the remote contents should be saved
     *
     * @throws IOException if there is a problem retrieving the file
     */
    @API(status = API.Status.STABLE)
    public void fetch(InetAddress player, String mountPath, String sourcePath, File destination) throws IOException {
        try {
            // Make sure the file exists on the player, and find its file handle.
            DirOpResBody found = find(player, mountPath, sourcePath);
            if (found.attributes.type != FType.NFREG) {
                throw new IOException("Path \"" + sourcePath + "\" is not a normal file.");
            }
            final FHandle handle = found.file;
            final int size = found.attributes.size;
            final int regions = regionCount(size);

            try (FileChannel channel = FileChannel.open(destination.toPath(),
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
                if (regions <= 1) {
                    readRegion(getNfsClient(player), handle, 0, size, channel, sourcePath);
                } else {
                    fetchInParallel(player, handle, size, regions, channel, sourcePath);
                }
            }
        } catch (OncRpcException e) {
            throw new IOException("Unable to download file \"" + sourcePath + "\", caught ONC RPC exception.", e);
        }
    }

    /**
     * Decide how many parallel regions to split a transfer of the given size into, honoring {@link #concurrency} but
     * never splitting so finely that regions fall below {@link #MIN_BYTES_PER_CONNECTION}.
     *
     * @param size the number of bytes to be transferred
     *
     * @return the number of regions (and therefore parallel clients) to use, at least one
     */
    private int regionCount(int size) {
        int byMinimum = (size + MIN_BYTES_PER_CONNECTION - 1) / MIN_BYTES_PER_CONNECTION;
        return Math.max(1, Math.min(concurrency, byMinimum));
    }

    /**
     * Download a file by splitting it into disjoint regions read concurrently, each over its own NFS client, writing
     * directly to the correct offset in the destination channel.
     *
     * @param player the address of the player from which we are retrieving the file
     * @param handle the NFS handle of the (already located) file
     * @param size the total size of the file in bytes
     * @param regions the number of regions to split it into (each gets its own client and thread)
     * @param channel the destination, written via position-based writes so the regions don't interfere
     * @param sourcePath the path of the file, for error messages
     *
     * @throws IOException if there is a problem retrieving the file
     * @throws OncRpcException if there is a problem in the remote procedure call layer
     */
    private void fetchInParallel(InetAddress player, FHandle handle, int size, int regions,
                                 FileChannel channel, String sourcePath) throws IOException, OncRpcException {
        List<OncRpcUdpClient> clients = getNfsClients(player, regions);
        int regionSize = (size + regions - 1) / regions;
        List<Callable<Void>> tasks = new ArrayList<>(regions);
        for (int i = 0; i < regions; i++) {
            final int start = i * regionSize;
            final int end = Math.min(size, start + regionSize);
            if (start >= end) {
                break;
            }
            final OncRpcUdpClient client = clients.get(i);
            tasks.add(() -> {
                readRegion(client, handle, start, end, channel, sourcePath);
                return null;
            });
        }

        ExecutorService pool = Executors.newFixedThreadPool(tasks.size());
        try {
            for (Future<Void> future : pool.invokeAll(tasks)) {
                future.get();  // Surfaces any failure from a region as an ExecutionException.
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while downloading \"" + sourcePath + "\".", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException) {
                throw (IOException) cause;
            }
            if (cause instanceof OncRpcException) {
                throw (OncRpcException) cause;
            }
            throw new IOException("Problem downloading \"" + sourcePath + "\".", cause);
        } finally {
            pool.shutdownNow();
        }
    }

    /**
     * Read the byte range {@code [start, end)} of an open NFS file handle, in {@link #readSize} chunks, writing each
     * chunk directly to its position in the destination channel. Tolerates short reads (advances by however many
     * bytes actually arrived).
     *
     * @param client the NFS client to use (must not be shared with another in-flight call)
     * @param handle the NFS handle of the file
     * @param start the first byte offset to read, inclusive
     * @param end the byte offset to stop at, exclusive
     * @param channel the destination, written via position-based writes
     * @param sourcePath the path of the file, for error messages
     *
     * @throws IOException if there is a problem retrieving or writing the data
     * @throws OncRpcException if there is a problem in the remote procedure call layer
     */
    private void readRegion(OncRpcUdpClient client, FHandle handle, int start, int end,
                            FileChannel channel, String sourcePath) throws IOException, OncRpcException {
        ReadArgs args = new ReadArgs();
        args.file = handle;
        int offset = start;
        while (offset < end) {
            args.offset = offset;
            args.count = Math.min(readSize, end - offset);
            ReadRes result = new ReadRes();
            client.call(nfs.NFSPROC_READ_2, args, result);
            if (result.status != Stat.NFS_OK) {
                throw new IOException("Problem reading \"" + sourcePath + "\": NFS read call returned status: " + result.status);
            }
            byte[] data = result.readResOk.data.value;
            if (data.length == 0) {
                throw new IOException("Problem reading \"" + sourcePath + "\": unexpected end of file at offset " + offset);
            }
            ByteBuffer buffer = ByteBuffer.wrap(data);
            int writeOffset = offset;
            while (buffer.hasRemaining()) {
                writeOffset += channel.write(buffer, writeOffset);
            }
            offset += data.length;
        }
    }
}
