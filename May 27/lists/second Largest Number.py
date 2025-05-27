import random

list_of_numbers=[random.randint(0,100) for x in range(random.randint(5,20))]

max_ele=max(list_of_numbers)
while(max_ele in list_of_numbers):
    list_of_numbers.remove(max_ele)
print(f"the seconf largest number is {max(list_of_numbers)}")
