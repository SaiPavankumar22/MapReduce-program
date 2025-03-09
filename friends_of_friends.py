from mrjob.job import MRJob
from mrjob.step import MRStep
import ast

class FriendsOfFriends(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_find_mutual_friends,
                   reducer=self.reducer_collect_mutual_friends),
            MRStep(mapper=self.mapper_count_mutuals,
                   reducer=self.reducer_rank_recommendations)
        ]

    def mapper_find_mutual_friends(self, _, line):
        user, friends = line.split("\t")
        friends = friends.split(",")
        
        for friend in friends:
            yield (user, friend), "DIRECT"

        for i in range(len(friends)):
            for j in range(i+1, len(friends)):
                friend_pair = tuple(sorted([friends[i], friends[j]]))
                yield friend_pair, user

    def reducer_collect_mutual_friends(self, friend_pair, mutual_friends):
        mutual_friends_list = list(mutual_friends)

        if "DIRECT" in mutual_friends_list:
            return

        yield friend_pair, mutual_friends_list

    def mapper_count_mutuals(self, friend_pair, mutual_friends):
        for mutual_friend in mutual_friends:
            for other_mutual in mutual_friends:
                if mutual_friend != other_mutual:
                    yield (mutual_friend, other_mutual), 1

    def reducer_rank_recommendations(self, friend_pair, counts):
        yield friend_pair[0], (friend_pair[1], sum(counts))

if _name_ == '_main_':
    FriendsOfFriends.run()
