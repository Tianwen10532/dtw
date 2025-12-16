
import dtw

@dtw.remote
class MyActor:
    def __init__(self,value):
        self.value=value
    def inc(self,num):
        self.value=self.value+num
        return self.value
    
