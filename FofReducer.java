import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;

public class FofReducerStage1 extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<String> commonFriends = new ArrayList<>();
        
        // Retrieve the list of friends for both users
        for (Text val : values) {
            commonFriends.add(val.toString());
        }
        
        // Calculate intersection of friend lists for both users
        String[] friendsUser1 = commonFriends.get(0).split(",");
        String[] friendsUser2 = commonFriends.get(1).split(",");
        
        List<String> common = new ArrayList<>();
        for (String friend1 : friendsUser1) {
            for (String friend2 : friendsUser2) {
                if (friend1.equals(friend2)) {
                    common.add(friend1);
                }
            }
        }
        
        // Emit the common friends
        context.write(key, new Text(String.join(",", common)));
    }
}

