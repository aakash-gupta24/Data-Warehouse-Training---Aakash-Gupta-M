def is_prime(x):
    if(x<2):
        return False
    for xx in range(2,int(x**0.5)+1):
        if(x%xx==0):
            return False
    return True
primes=[]
for x in range(1,101):
    if(is_prime(x)):
        primes.append(x)
print(*primes,"are the primes from 1 too 100",sep=",")
