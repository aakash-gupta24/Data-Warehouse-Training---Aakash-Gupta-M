numbers=list(map(int,list(input("enter the number:").strip())))
print(*numbers,sep="+",end="")
print("=",sum(numbers))
