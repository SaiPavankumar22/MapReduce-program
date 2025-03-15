import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class FofMapperStage2 extends Mapper<Object, Text, Text, Text> {

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Input is in the form of userA,userB <tab> commonFriends
        String[] parts = value.toString().split("\t");
        String[] userPair = parts[0].split(",");
        String commonFriends = parts[1];
        
        String[] friends = commonFriends.split(",");
        for (String friend : friends) {
            // Emit friend with count of common friends
            context.write(new Text(friend), new Text(userPair[0] + "," + userPair[1] + "," + commonFriends));
        }
    }
}

