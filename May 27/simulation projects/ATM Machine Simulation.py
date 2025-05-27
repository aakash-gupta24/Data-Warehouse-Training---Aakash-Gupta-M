balance = 10000

def atm_menu():
    print("\n=== ATM MENU ===")
    print("1. Deposit")
    print("2. Withdraw")
    print("3. Check Balance")
    print("4. Exit")

while True:
    atm_menu()
    choice = input("Select an option (1-4): ")

    if choice == '1':
        try:
            amount = float(input("Enter amount to deposit: "))
            if amount > 0:
                balance += amount
                print(f"{amount:.2f} deposited successfully.")
            else:
                print("Enter a positive amount.")
        except ValueError:
            print("Invalid input. Please enter a number.")

    elif choice == '2':
        try:
            amount = float(input("Enter amount to withdraw: "))
            if amount > balance:
                print("Insufficient balance.")
            elif amount <= 0:
                print("Enter a positive amount.")
            else:
                balance -= amount
                print(f"{amount:.2f} withdrawn successfully.")
        except ValueError:
            print("Invalid input. Please enter a number.")

    elif choice == '3':
        print(f"Your current balance is: {balance:.2f}")

    elif choice == '4':
        print("Thank you for using the ATM. Goodbye!")
        break

    else:
        print("Invalid choice. Please select a valid option (1-4).")
