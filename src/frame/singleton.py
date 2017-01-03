class singleton(object):  
    def __new__(cls, *args, **kw):  
        if not hasattr(cls, '_instance'):  
            orig = super(singleton, cls)  
            cls._instance = orig.__new__(cls, *args, **kw)  
        return cls._instance  
