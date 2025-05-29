class BankAccount:
    def __init__(self, name, balance=0):
        self.name = name
        self.balance = balance

    def deposit(self, amount):
        if amount > 0:
            self.balance += amount

    def withdraw(self, amount):
        if 0 < amount <= self.balance:
            self.balance -= amount
        else:
            print("Insufficient balance.")

    def get_balance(self):
        return self.balance

name = input("Enter account holder's name: ")
initial_balance = float(input("Enter initial balance: "))

account = BankAccount(name, initial_balance)
while True:
    print("1 deposit")
    print("2 Withdraw")
    print("3 Balance")
    print("-1 Exit")
    opt=int(input("Enter choice (1/2)"))
    if(opt==1):
        dep_amt=int(input("Enter the amount to deposit:"))
        account.deposit(dep_amt)
    elif(opt==2):
        wit_amt = int(input("Enter the amount to withdraw:"))
        account.withdraw(500)
    elif (opt == 3):
        print(f"Current balance: {account.get_balance()}")
    elif(opt==-1):
        break
    else:
        print("Enter a valid option")
