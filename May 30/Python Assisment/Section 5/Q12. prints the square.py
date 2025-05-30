try:
    number=int(input("Enter the number to square :"))
    print(f"The square of number {number} is {number**2}")

except ValueError as e:
    print("Enter a number!!! Invalid input.")
