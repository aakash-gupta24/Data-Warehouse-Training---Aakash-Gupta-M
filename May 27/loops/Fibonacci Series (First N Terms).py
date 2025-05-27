n=int(input("Enter the number of Fibonacci Series: "))
fib=[1,2]
if(0<=n<=2):
    print([fib[x] for x in range(n)])
elif(n>2):
    for x in range(n-2):
        fib.append(fib[-1]+fib[-2])
    print(fib)
else:
    print("Invalid input")