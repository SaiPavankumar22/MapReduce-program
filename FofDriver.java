import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;

public class FofMapperStage1 extends Mapper<Object, Text, Text, Text> {

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Input is in the form of "userID <tab> friend1,friend2,..."
        String[] parts = value.toString().split("\t");
        String user = parts[0]; // user ID
        String friendsList = parts[1]; // comma-separated list of friends
        
        // Convert the friends list into an array
        String[] friends = friendsList.split(",");
        
        // Generate pairs of users to emit (combinations of user and their friends)
        for (String friend : friends) {
            String pairKey = user.compareTo(friend) < 0 ? user + "," + friend : friend + "," + user;
            context.write(new Text(pairKey), new Text(friendsList));
        }
    }
}

