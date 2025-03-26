
class Penson:

    def __init__(self, name="renjie", age=18):
        self.name = name
        self.age = age

    def readTheDict(self, **dic):
        "print the dic param"
        print(dic)

    def writeTheDict(self):
        dict={"a":"b","c":"d","e":"f"}
        self.readTheDict(**dict)

if __name__=='__main__':
    Penson=Penson()
    Penson.writeTheDict()
