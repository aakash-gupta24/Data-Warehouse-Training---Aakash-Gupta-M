numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
print(f"Original list :{numbers}")
for x in range(len(numbers)):
    if(numbers[x]%2==0):
        numbers[x]**=2
print(f"Modified list (doubled even) :{numbers}")
