def singleton(cls, *args, **kw):  
    instances = {}
    def _singleton(*args, **kw): 
        if cls not in instances:  
            instances[cls] = cls(*args, **kw)  
        return instances[cls]  
    return _singleton

if __name__ == "__main__":

    @singleton
    class singleton_test(object):
        def __init__(self, s_data):
            self._data = s_data
 
        def run(self):
            print self._data

    a = singleton_test("AAAAAAA")
    print a
    a.run()
    b = singleton_test("BBBBBBB")
    print b
    b.run()
