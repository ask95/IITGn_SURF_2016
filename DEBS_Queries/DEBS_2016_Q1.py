import copy
import datetime as dt
from Operators import *
from check_agent_parameter_types import *

DEFAULT = 10
K = 3
N = 5
MAX_SCORE = 1000000

class friendship():
    def __init__(self, input_ts, u1, u2):
        self.ts = input_ts
        self.user_id_1 = u1
        self.user_id_2 = u2

class post():
    def __init__(self, input_ts, input_postid, input_userid):
        self.ts = input_ts
        self.post_id = input_postid
        self.user_id = input_userid

class comment():
    def __init__(self, input_ts, input_commentid, input_userid, cr, pc):
        self.ts = input_ts
        self.comment_id = input_commentid
        self.user_id = input_userid
        self.comment_replied = cr
        self.post_commented = pc

class like():
    def __init__(self, input_ts, input_userid, input_commentid):
        self.ts = input_ts
        self.user_id = input_userid
        self.comment_id = input_commentid


class top_scoring_posts():

    '''
    1. from text to required data type for the attributes (of post, comment, etc) while receiving inputs
    2. to have a data loader that ensures feeding of data in correct sequence acc to timestamp

    '''
    
    """
    Attributes
    ----------
    score_to_posts: list
       score_to_posts[j] is the set of posts that have a score of j or less.
    posts: dict
       key: post_id
       value: list of:
            0. The last updated score for this post_id
            1. The latest day on which 1 was subtracted from the score of this post.
            2. The time (hours, minutes, seconds,...) of this post
            3. The set of comment ids associated with this post.
    comments: dict
        key: comment_id
        value: list of:
            0. The last updated score for this comment.
            1. The latest day on which 1 was subtracted from the score of this comment.
            2  The time (hours, minutes, seconds,...) of this comment.
            3. The post associated with this comment.
    highest_scores: list of at most N integers
        highest_scores[j] is an integer k such that scores_to_posts[k] is a nonempty set,
        and there are j nonempty sets in the interval scores_to_posts[0:k].
    timestamps: circular list
        An element of the list is a list (time, is_post, post_id).
        The time component of the list is hour, minute, second, sub-second. (The day is
        not part of the time component.)
        The list is ordered in non-decreasing order of time. We treat the list as a circular
        list (even though it is implemented as a regular Python linear list).
        The id component of the list is an id of one of the top-k posts or its comment.
        Each one of the top-k posts and comments connected to them appears in timestamps.

    clock: time
        The current time in days, hours, minutes, seconds, subseconds,...

    Methods
    -------
    procedure_timestamp(new_time)
        Decrement scores of top-k posts with timestamps in the interval [clock, new_time]
        If no top-k post exists in this interval then take no further action.
        If top-k posts exist in this interval then process highest_scores in order. For
        the slot in score pointed to by highest_scores:
          . Compute the correct scores for posts in the set corresponding to this
          highest_score cell.
          . Move these posts to the appropriate cells in score_to_post.
          . Update the list, highest_scores.
          . If the top-k posts have been found stop; else continue to the next cell
              pointed to by highest_scores.
          . Check invariants
          
        Also update the scores structure and make changes to highest_scores if necessary
        
    process_post(new_post)
        INPUT: A tuple having the following fields (a) post_id (b)ts
        This method is executed whenever a new post arrives
        
    process_comment(new_comment)
        INPUT: A tuple having the following fields (a) comment_id (b)ts (c)comment_replied (d)post_replied
	This method is executed whenever a new comment arrives
	
    total_score(post_id)
        for a particular post, it returns the total score(sum of the scores of post and the active comments associated with it)
    
    score_update(item, time)
        for a particular post/comment, it updates its score in reference to the 'time'.  
        This function is only for decrements as increment of +10 is done during the initialization of the
        post/comment itself

    updated_score: Input post_id and Time:
    Score of the this post_id and its corresponding scores are updated (If comments are found to be outdated they are removed) 

    """
    def __init__(self, input_post_stream, window_size, step_size):
        self.score_to_posts = [set()]*(MAX_SCORE+1) #bucket
        self.posts = {}
        self.comments = {}
        self.highest_scores = []
        self.timestamps = []
        self.clock = 0
        self.input_posts = input_post_stream
        self.input_comments = input_comment_stream
        self.agent_newPost = list_agent(self.process_post, self.input_posts, [], None, None, window_size, step_size)
        self.agent_newComment = list_agent(self.process_comment, self.input_comments, [], None, None, window_size, step_size)
	

    def process_post(self, new_post):

        assert isinstance(new_post, post), \
          'Input Error: object in input is not a post.'
        
        procedure_timestamp(new_post.ts)
        self.posts.update({new_post.post_id: [DEFAULT, new_post.ts.date(), new_post.ts.time(),set()]})
        self.score_to_posts[DEFAULT].add(new_post.id)
        if (self.highest_scores == []):
            self.highest_scores = [DEFAULT]
        elif (DEFAULT > self.highest_scores[0]):
            if (len(self.highest_scores) < N):
                self.highest_scores = [DEFAULT]+self.highest_scores
            else:
                self.highest_scores = [DEFAULT]+self.highest_scores[:(N-1)]
        elif (DEFAULT > self.highest_scores[-1]):
            if DEFAULT in self.highest_scores:
                pass
            else:
                self.highest_scores.append(DEFAULT)
                self.highest_scores.sort()
        self.clock = new_post.ts.time()
        if DEFAULT in self.highest_scores:
            index = 0
            for i in range(len(self.timestamps)):
                if (self.timestamps[i][0] <=new_post.ts.time()):
                    index = i
                else:
                    break
            self.timestamps.insert(index,(new_post.ts.time(), \
                                          True,new_post.post_id))
        return [], None

    def process_comment(self,new_comment):
        procedure_timestamp(new_comment.ts)
        assert isinstance(new_comment, comment), \
          'Input Error: object in input is not a comment.'
        parent_post = None
        if (new_comment.comment_replied == -1):
            parent_post = new_comment.post_commented
            assert isinstance(parent_post, post), \
                   'Input Error: object retrieved is not a post.'
            if parent_post in self.posts:
                self.comments.update({new_comment.comment_id: \
                    [DEFAULT, new_comment.ts.date(), new_comment.ts.time(),\
                         parent_post]})
        else:
            try:
                parent_comment = new_comment.comment_replied
                parent_post = comments[parent_comment][3]
                self.comments.update({new_comment.comment_id: \
                [DEFAULT, new_comment.ts.day,new_comment.ts.time, parent_post]})
            except:
                pass
        try:
            old_score = self.total_score(parent_post)
            self.score_to_posts[old_score].discard(parent_post)
            self.updated_score(parent_post)
            self.posts[parent_post][3].add(new_comment.comment_id)
            new_total_score = self.total_score(parent_post)
            self.score_to_posts[new_score].add(parent_post)
            ### Update timestamps and highest_scores
            if (new_total_score > self.highest_scores[0]):
                if (len(self.highest_scores) < N):
                    self.highest_scores = [DEFAULT]+self.highest_scores
                else:
                    self.highest_scores = [DEFAULT]+self.highest_scores[:(N-1)]



            
########            if (new_total_score == DEFAULT):
########                for i in self.posts[parent_post][3]:
########                    self.comments.pop(i, None)
########                self.posts.pop(parent_post,None)
########                self.score_to_posts[old_score].discard(parent_post)
########            else:
########                self.score_to_posts[old_score].discard(parent_post)
########                self.score_to_posts[new_score].add(parent_post)
########                if (new_total_score > self.highest_scores[0]):
########                    if (len(self.highest_scores) < N):
########                        self.highest_scores = [DEFAULT]+self.highest_scores
########                    else:
########                        self.highest_scores = [DEFAULT]+self.highest_scores[:(N-1)]
                        ### ADD timestamps
                        
        except:
            pass
        self.clock = new_comment.ts
        return [], None

        
    def procedure_timestamp(self,new_time):
        assert isinstance(new_time, datetime), \
          'Input Error: timestamp provided is not datetime .'
        
        old_time = self.clock.time()
        new_time = new_time.time()
        start_index = 0
        end_index = 0
        for i in range(len(self.timestamps)):
            if (self.timestamps[i][0] <= old_time):
                start_index = i
            if (self.timestamps[i][0] <= new_time):
                end_index = i

        if (start_index > end_index):
            time_lst = self.timestamps[end_index:] + self.timestamps[:start_index]
        else:
            time_lst = self.timestamps[start_index:end_index]
        for j in range(len(time_lst)):
            item = time_lst[j]
            if item[1]:
                subtract = score_update(item[2] ,new_time,1) # post.score invariant maintained, as only subtractions can occur
                new_score = total_score(item[2]) 
                if (subtract != 0): #highest_score, top _k_posts,timestamps are momentarily disturbed (as the following lines are for pointer allocation)
                    old_score = new_score - subtract 
                    score_to_posts[new_score].add(item[2])    # score_to_posts invariant maintained (as the new score which is added to can only be less than old score)
                    score_to_posts[old_score].discard(item[2]) # score_to_posts invariant maintained

            else:
                subtract = score_update(item[2], new_time,0)         #comment.score invariant maintained
                new_score = total_score(comments[item[2]][3])
                if (subtract != 0):
                    score_to_posts[new_score].add(comments[item[2]][3])
                    score_to_posts[old_score].discard(comments[item[2]][3])
                    #score_to_posts invariant maintained, due to reason mentioned in line 136   
                       
            repl_list = copy.deepcopy(self.highest_scores)
            repl_list = update_highest(repl_list)
            self.highest_scores = copy.deepcopy(repl_list)
            if (len(highest_scores) > 3):
                top_k_scores = highest_scores[:3]
            else:
                top_k_scores = highest_scores

            check = 0
            for score in self.highest_scores:
                for post_list in self.score_to_posts[score]:
                    for post_id in post_list:
                        post_time = self.posts[post_id].ts.time
                        self.timestamps.append([post_time, 1, post_id])
                        for comment_id in self.posts[post_id][3]:
                            self.timestamps.append([self.comments[comment_id][2], 0, comment_id])
                        check += 1
                    #TO handle the case of equality in the minimum score among the k posts, timestamps
                        #may include (slightly) more than k posts
                    if (check == K) or (check > K):
                        break
                if (check == K) or (check > K):
                    break

            self.timestamps.sort(key=lambda x: x[0])                                    
            #update timestamps as per top_k_scores
            # top_k_posts,timestamps are updated
        return None

    def total_score(self,post_id):
        post = self.posts[post_id]
        score = post[0]
        for comment_id in post[3]:
            score += self.comments[comment_id][0]
        return score

    def score_update(self,item,p_time,is_post):
        if is_post:
            Date = self.posts[item][1]
            self.posts[post_id][1] = p_time.date()
            Time = self.posts[post_id][2]
            time_stmp = dt.datetime.combine(Date,Time)
            delta = p_time - time_stmp
            if (delta.year != 0) or (delta.month != 0):
                substract= self.posts[post_id][0]
                self.posts[post_id][0] = 0
            else:
                substract = delta.day
                temp = self.posts[post_id][0]
                self.posts[post_id][0] = max (0,self.posts[post_id][0]-substract)
                substract = temp-self.posts[post_id][0]
            return substract
        else:
            Date = self.comments[item][1]
            self.comments[item][1] = p_time.date()
            Time = self.comments[item][2]
            time_stmp = dt.datetime.combine(Date,Time)
            delta = p_time - time_stmp
            if (delta.year != 0) or (delta.month != 0):
                substract= self.posts[item][0]
                self.posts[item][0] = 0
            else:
                substract = delta.day
                temp = self.comments[item][0]
                self.comments[item][0] = max (0,self.comments[item][0]-substract)
                substract = temp-self.comments[item][0]
                if (self.comments[item][0] == 0):
                    post_id = self.comments[item][3]
                    self.posts[post_id][3].discard(item)
            return substract

    def update_highest(self, input_list):
        index = -1
        topk = 0
        temp = []
        while (topk <= K) and (index+1 in range(len(input_list))) and (index+2 in range(len(input_list))):
            index = index+1
            for i in range(input_list[index+1],input_list[index]):
                post = score_to_posts[input_list[index]]
                if (post != set()):
                    temp.append(i)
                    topk = topk+1
        temp = temp +input_list[index+1:]
        if (len(temp) > N):
            temp =temp[:N]    
        return temp
    
    def updated_score(self,post_id,tme):
        if (self.posts[post_id][0] > 0):
            Date = self.posts[post_id][1]
            self.posts[post_id][1] = tme.date()
            Time = self.posts[post_id][2]
            time_stmp = dt.datetime.combine(Date,Time)
            delta = tme - time_stmp
            if (delta.year != 0) or (delta.month != 0) or (delta.day > 10):
                self.posts[post_id][0] = 0  
            else:
                substract = delta.day
                self.posts[post_id][0] = max(0, self.posts[post_id][0]-substract)       
        for i in self.posts[post_id][3]:
            Date = self.comments[i][1]
            Time = self.comments[i][2]
            time_stmp = dt.datetime.combine(Date,Time)
            delta = tme - time_stmp
            if (delta.year != 0) or (delta.month != 0) or (delta.day > 10):
                self.comments[i][0] = 0
            else:
                substract = delta.day
                self.comments[i][0] = max (0,self.posts[post_id][0]-substract)
            if (self.comments[i][0] == 0):
                self.posts[post_id][3].discard(i)

        if (self.posts[post_id][0] == 0) and (self.posts[post_id][3] == set()):
            self.posts.pop(post_id, None)
        return
        
