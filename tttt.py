print('f')

#adding extra keys
print("%(PARG_1)d %(PARG_2)d" % {'PARG_1': 1, 'PARG_2':2, 'PARG_3':3})

#conside enumeration
obj = [1,2,3]
for index in range(len(obj)):
    value = obj[index]
    print(value)

#conside enumeration
obj = [1,2,3]
for index, value in enumerate(obj):
    print(index, value) 

# exception
try:
    1 / 0
except Exception:
    print("ex")
except ZeroDivisionError:
    print("don't divide numbers by 0!")

# exception
try:
    1 / 0
except ZeroDivisionError:
    print("don't divide numbers by 0!")  
except Exception:
    print("exception occurred")
  