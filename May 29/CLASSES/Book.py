class Book:
    def __init__(self, title, author, price, in_stock):
        self.title = title
        self.author = author
        self.price = price
        self.in_stock = in_stock

    def sell(self, quantity):
        if quantity <= self.in_stock:
            self.in_stock -= quantity
        else:
            raise ValueError("Not enough stock to complete the sale.")

title = input("Enter book title: ")
author = input("Enter author name: ")
price = float(input("Enter book price: "))
stock = int(input("Enter stock quantity: "))

book = Book(title, author, price, stock)

try:
    quantity = int(input("Enter quantity to sell: "))
    book.sell(quantity)
    print(f"Sale successful. Remaining stock: {book.in_stock}")
except ValueError as e:
    print(e)
