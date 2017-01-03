from abc import ABCMeta,abstractmethod
class job_base:
    __metaclass__ = ABCMeta
    @abstractmethod
    def run(self):
        pass
