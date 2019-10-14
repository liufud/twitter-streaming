from tweepy import OAuthHandler, Stream, StreamListener
from kafka import SimpleProducer, KafkaClient

# Go to http://apps.twitter.com and create an app.
# The consumer key and secret will be generated for you after
consumer_key="PvzTa2HfsMuzm5hKabXFUdLqi"
consumer_secret="bhBP58nvB0Tvr1yErJgNOBA2fWeYQZnUeTA2Dqlb6KVOZdtXN3"

# After the step above, you will be redirected to your app's page.
# Create an access token under the the "Your access token" section
access_token="1179970574719864832-AxCLHhi78tksfC9PgYz4iAnBl2IV31"
access_token_secret="HjnSDRL6VFVZpRvv025lqwEQDSH4B0VBaDfjT1IKxafpP"

class KafkaListener(StreamListener):
    """ A listener handles tweets that are received from the stream.
    This is a basic listener that just prints received tweets to stdout.
    """
    def on_data(self, data):
        producer.send_messages("twitter-stream", data.encode("utf-8"))
	return True

    def on_error(self, status):
        print(status)

if __name__ == '__main__':
    kafka_client = KafkaClient("localhost:9092")
    producer = SimpleProducer(kafka_client)


    l = KafkaListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    stream = Stream(auth, l)
    stream.filter(track=['#'])
