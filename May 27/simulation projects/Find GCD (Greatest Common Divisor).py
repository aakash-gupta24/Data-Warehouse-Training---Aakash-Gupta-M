def find_gcd(a, b):
    while b != 0:
        a, b = b, a % b
    return a


try:
    num1 = int(input("Enter the first number: "))
    num2 = int(input("Enter the second number: "))
    gcd = find_gcd(abs(num1), abs(num2))
    print(f"The GCD of {num1} and {num2} is {gcd}.")
except ValueError:
    print("Please enter valid integers.")
