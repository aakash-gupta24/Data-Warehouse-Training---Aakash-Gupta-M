def is_even(x):
    return x%2==0

odd=[]
even=[]

starting=int(input("Enter the starting range :"))
ending=int(input("Enter the ending range :"))

for x in range(starting,ending):
    if(is_even(x)):
        even.append(x)
    else:
        odd.append(x)
print(f"In range {starting}-{ending}:")
print(f"Odd numberers are {odd}")

