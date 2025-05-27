string=input("Enter the palindrome :")

palin_flag=1
for x in range(len(string)):
    if(string[x]!=string[len(string)-x-1]):
        print(f"{string} is not a palindrome")
        palin_flag=0
        break
if(palin_flag==1):
    print(f"{string} is a palindrome")
