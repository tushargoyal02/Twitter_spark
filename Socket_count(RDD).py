#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark import SparkContext,SparkConf


# In[2]:


from pyspark.streaming import StreamingContext


# In[3]:


#conf = SparkConf().setMaster("local").setAppName("CountingWord")


# In[6]:


sc.stop()


# In[7]:


sc = SparkContext("local[2]",'words')


# In[8]:


#sc = SparkContext(conf=conf)


# In[9]:


# in batches of 1 second(input stream batches)

ssc = StreamingContext(sc,1)


# In[10]:


# here we are defining the port for the localhost 

lines = ssc.socketTextStream('localhost',9998)


# In[13]:



# we are splitting the line with the split function on the basis of space
words = lines.flatMap(lambda line: line.split( " " ))


# In[14]:


# here we are making the tuple for each words as for like hello => (hello,1)

pairs = words.map(lambda words : (words,1))


# In[15]:


# it takes the same number of key and will reduce it (hello,1) ,(hello,1) => (hello,2)
words_count = pairs.reduceByKey(lambda numb1,numb2 : numb1+numb2)


# In[16]:


words_count.pprint()


# In[17]:


ssc.start()


# In[95]:


##
    # type => nc -lk 9999  (before starting the ssc.start)
    
# => CHANGE THE PORT NUMBER IF DOESNT WORK!


# In[ ]:





# In[ ]:




