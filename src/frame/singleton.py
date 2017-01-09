instances = {}
def singleton(cls, *args, **kw):
    global instances
    def _singleton(*args, **kw): 
        if cls.__name__ not in instances:  
            instances[cls.__name__] = cls(*args, **kw)
        return instances[cls.__name__]  
    return _singleton

if __name__ == "__main__":

    @singleton
    class singleton_test(object):
        def __init__(self, s_data):
            print "init"
            self._data = s_data
 
        def run(self):
            print self._data

    a = singleton_test("AAAAAAA")
    print a
    a.run()
    b = singleton_test("BBBBBBB")
    print b
    b.run()
