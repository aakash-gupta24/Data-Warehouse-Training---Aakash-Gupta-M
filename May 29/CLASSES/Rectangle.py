class Rectangle():
    def __init__(self,length,width):
        self.length=length
        self.width=width
    def area(self):
        return self.length*self.width
    def perimeter(self):
        return 2 * (self.length + self.width)

    def is_square(self):
        return self.length == self.width

try:
    length = float(input("Enter the length: "))
    width = float(input("Enter the width: "))
    rect = Rectangle(length, width)
    print(f"Area: {rect.area()}")
    print(f"Perimeter: {rect.perimeter()}")
    print(f"Is square: {rect.is_square()}")
except Exception as e:
    print(e)