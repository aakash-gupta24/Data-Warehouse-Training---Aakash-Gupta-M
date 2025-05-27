import string

def check_password_strength(password):
    if len(password) < 8:
        return False, "Password must be at least 8 characters long."

    if not any(char.isdigit() for char in password):
        return False, "Password must contain at least one number."

    if not any(char.isupper() for char in password):
        return False, "Password must contain at least one uppercase letter."

    if not any(char in string.punctuation for char in password):
        return False, "Password must contain at least one special symbol."

    return True, "Password is strong."
while True:
    pwd = input("Enter your password: ")
    valid, message = check_password_strength(pwd)
    print(message)
    if(not int(input("\nwant to check more? (0/1):"))):
        break

print("Thank you, see you soon!!")