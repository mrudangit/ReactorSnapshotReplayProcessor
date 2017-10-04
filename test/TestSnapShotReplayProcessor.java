import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.SpringBootTest;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.mm.SnapshotReplayProcessor;

import java.util.HashMap;

@SpringBootTest
public class TestSnapShotReplayProcessor {

    private static final Logger logger = LoggerFactory.getLogger(TestSnapShotReplayProcessor.class);

    private Object KeyFunction(HashMap<String,String> s){

        return  s.get("Key");
    }


    private HashMap<String,String> MergeFunction(HashMap<String,String> original, HashMap<String,String> newItem){

        original.putAll(newItem);
        return  original;
    }

    @Test
    public void contextLoads() {

        logger.info("Test Started");

        final int[] sub1Count = {0};
        final int[] sub2Count = {0};

        final int[] sub3Count = {0};

        SnapshotReplayProcessor<HashMap<String,String>> s = new SnapshotReplayProcessor<>(this::KeyFunction,this::MergeFunction);

        HashMap<String,String> data = new HashMap<>();
        data.put("Key","IBM");
        data.put("Price","100");



        Disposable disposable = s.publishOn(Schedulers.newSingle("mm")).subscribe(s1 -> {
            logger.info("Subscription1 = {}", s1);
            sub1Count[0]++;
        });


        Disposable disposable2 = s.subscribe(s1 -> {
            logger.info("Subscription2 = {}", s1);
            sub2Count[0]++;
        });



        Flux.range(1,10).subscribe(integer -> {
            if( integer == 5){
                disposable2.dispose();
            }
            data.put("LastPrice",integer.toString());
            s.onNext(data);
        });




        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }



        Disposable disposable3 = s.subscribe(s1 -> {
            logger.info("Subscription3 = {}", s1);
            sub3Count[0]++;
            Assert.assertEquals(s1.get("LastPrice"),"10");
        });


        Assert.assertEquals(sub1Count[0],10);
        Assert.assertEquals(sub2Count[0],4);
        Assert.assertEquals(sub3Count[0],1);

    }

}
