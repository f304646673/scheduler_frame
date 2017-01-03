#coding=utf-8 
import types
import re

def IsNumber(varObj):
    return type(varObj) is types.IntType

def IsString(varObj):
    return type(varObj) is types.StringType

def IsFloat(varObj):
    return type(varObj) is types.FloatType

def IsDict(varObj):
    return type(varObj) is types.DictType

def IsTuple(varObj):
    return type(varObj) is types.TupleType

def IsList(varObj):
    return type(varObj) is types.ListType

def IsBoolean(varObj):
    return type(varObj) is types.BooleanType

def IsCurrency(varObj):
    if IsFloat(varObj) and IsNumber(varObj):
        if varObj >0:
            return isNumber(currencyObj)
        return False
    return True

def IsEmpty(varObj):
    if len(varObj) == 0:
        return True
    return False

def IsNone(varObj):
    return type(varObj) is types.NoneType# == "None" or varObj == "none":

def IsDate(varObj):
    if len(varObj) == 10:
        rule = '(([0-9]{3}[1-9]|[0-9]{2}[1-9][0-9]{1}|[0-9]{1}[1-9][0-9]{2}|[1-9][0-9]{3})-(((0[13578]|1[02])-(0[1-9]|[12][0-9]|3[01]))|((0[469]|11)-(0[1-9]|[12][0-9]|30))|(02-(0[1-9]|[1][0-9]|2[0-8]))))|((([0-9]{2})(0[48]|[2468][048]|[13579][26])|((0[48]|[2468][048]|[3579][26])00))-02-29)$/'
        match = re.match( rule , varObj )
        if match:
            return True
        return False
    return False

def IsEmail(varObj):
    rule = '[\w-]+(\.[\w-]+)*@[\w-]+(\.[\w-]+)+$'
    match = re.match( rule , varObj )

    if match:
        return True
    return False

def IsChineseCharString(varObj):
    for x in varObj:
        if (x >= u"\u4e00" and x<=u"\u9fa5") or (x >= u'\u0041' and x<=u'\u005a') or (x >= u'\u0061' and x<=u'\u007a'):
           continue
        else:
            return False
    return True


def IsChineseChar(varObj):
    if varObj[0] > chr(127):
        return True
    return False

def IsLegalAccounts(varObj):
    rule = '[a-zA-Z][a-zA-Z0-9_]{3,15}$'
    match = re.match( rule , varObj )

    if match:
        return True
    return False

def IsIpAddr(varObj):
    rule = '\d+\.\d+\.\d+\.\d+'
    match = re.match( rule , varObj )

    if match:
        return True
    return False

if __name__=='__main__':
    print 'IsDate:',IsDate('2010-01-31')
    print 'IsNone:',IsNone(None)
    print 'IsEmpty:',IsEmpty('1')
    print 'IsCurrency:',IsCurrency(1.32)
    print 'IsList:',IsList([1,3,4])
    print 'IsTuple:',IsTuple([1,3,4])
    print 'IsDict:',IsDict({'a1':'1','a2':'2'})
    print 'IsFloat:',IsFloat(1.2)
    print 'IsString:',IsString('string')
    print 'IsNumber:',IsNumber(15)
    print 'IsEmail:',IsEmail('sgicer@163.com')
    print 'IsChineseChar:',IsChineseChar(u'Ö')
    print 'IsChineseCharString:',IsChineseCharString(u'Ö¹ú')
    print 'IsLegalAccounts:',IsLegalAccounts('alan_z')
    print 'IsIpAddr:',IsIpAddr('127.1110.0.1')
    pass
