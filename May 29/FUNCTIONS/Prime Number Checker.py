def is_prime(n):
    if(n<2):
        return False
    for xx in range(2,int(n**0.5)+1):
        if(n%xx==0):
            return False
    return True

num=int(input("Enter the number to check prime :"))
if(is_prime(num)):
    print(f"{num} is prime")
else:
    print(f"{num} is not prime")