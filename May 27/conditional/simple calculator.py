num1=int(input("Enter the first number:"))
num2=int(input("Enter the second number:"))
oper=input("Enter the operator [+,-,*,/]:")

if(oper in list("+-*/")):
    if(oper=="+"):
        print(f"{num1}+{num2}={num1+num2}")
    elif(oper=="-"):
        print(f"{num1}-{num2}={num1 - num2}")
    elif(oper=="*"):
        print(f"{num1}*{num2}={num1 * num2}")
    elif(oper=="/"):
        if(num2!=0):
            print(f"{num1}/{num2}={num1 / num2}")
        else:
            print("Error: Divided by 0")
else:
    print("Invalid operator")

