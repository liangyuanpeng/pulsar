package org.apache.pulsar.metadata;

import lombok.extern.slf4j.Slf4j;
import org.junit.*;
import org.junit.runners.MethodSorters;
import org.rocksdb.*;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class RocksdbTest {

    String url = "metadata-store:rocksdb://H:/repo/git/pulsar/data/metadata";
    @Test
    public void t0(){
//        url = "metadata-store:rocksdb://H:/repo/git/pulsar/data/metadata";
        URI uri = URI.create(url);
        log.info("success");
    }

    RocksDB rocksDB;

    Statistics statistics;

    @Before
    public void setup() throws RocksDBException {
        Options options = new Options();

        options.setBlobFileSize(123l);
        options.setEnableBlobFiles(true);
        options.setEnableBlobGarbageCollection(true);
        options.setMinBlobSize(123l);
        options.setBlobCompressionType(CompressionType.LZ4_COMPRESSION);
        statistics = new Statistics();
        statistics.setStatsLevel(StatsLevel.ALL);
        options.setStatistics(statistics);

        for (TickerType tickerType : TickerType.values()) {
            if (tickerType == TickerType.TICKER_ENUM_MAX) {
                continue;
            }
            //https://github.com/dremio/dremio-oss/blob/d41cb52143b6b0289fc8ed4d970bfcf410a669e8/services/datastore/src/main/java/com/dremio/datastore/ByteStoreManager.java#L273
//            Metrics.newGauge(Metrics.join(METRICS_PREFIX, tickerType.name()), () -> statistics.getTickerCount(tickerType));
        }

        options.setCreateIfMissing(true);
//        configLog(options);
        String dataPath = "H:/repo/git/pulsar/data/metadata";
        DBOptions dbOptions = new DBOptions();
        List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
        List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
//        RocksDB rocksDB =  TransactionDB.openReadOnly(dbOptions, dataPath, cfDescriptors, cfHandles);
        TransactionDB.loadLibrary();
        rocksDB = TransactionDB.open(dataPath);
    }

    @Test
    public void t1openReadOnly() throws RocksDBException {
        // https://www.freesion.com/article/89591464076/
        RocksIterator rocksIterator = rocksDB.newIterator();
        for (rocksIterator.seekToFirst(); rocksIterator.isValid(); rocksIterator.next()) {
            log.info("rockskey&value:{}|{}",new String(rocksIterator.key()),new String(rocksIterator.value()));
//            System.out.println(new String(rocksIterator.key()) + ":" + new String(rocksIterator.value()));
        }
//        byte[] bytes = rocksDB.get("/admin/v2/brokers/configuration".getBytes());
//        log.info("data:{}",new String(bytes));
    }

    @Test
    public void t2put() throws RocksDBException {
        rocksDB.put("hello".getBytes(),"world".getBytes());
    }

    @Test
    public void t3get() throws RocksDBException {
        Assert.assertEquals(new String(rocksDB.get("hello".getBytes())),"world");
    }

    @Test
    public void t4readStatistics() throws RocksDBException, InterruptedException {
        rocksDB.put("hello".getBytes(),"world".getBytes());
        statistics.reset();
        rocksDB.put("hello".getBytes(),"world".getBytes());

        rocksDB.flushWal(true);
        TimeUnit.SECONDS.sleep(60);
        log.info(String.valueOf(statistics.getAndResetTickerCount(TickerType.WRITE_DONE_BY_OTHER)));
        log.info(statistics.getHistogramString(HistogramType.HISTOGRAM_ENUM_MAX));
        log.info(String.valueOf(statistics.getTickerCount(TickerType.BLOB_DB_BLOB_FILE_BYTES_READ)));

        log.info(statistics.getHistogramString(HistogramType.BLOB_DB_BLOB_FILE_WRITE_MICROS));
        log.info(statistics.getHistogramString(HistogramType.BLOB_DB_BLOB_FILE_SYNC_MICROS));

    }

    @After
    public void close(){
        if(rocksDB!=null){
            rocksDB.close();
        }
    }
}
