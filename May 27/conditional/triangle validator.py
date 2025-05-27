def is_valid_triangle(a, b, c):
    return (a + b > c) and (a + c > b) and (b + c > a)
side1=int(input("Enter the first side:"))
side2=int(input("Enter the second side:"))
side3=int(input("Enter the third side:"))
if(is_valid_triangle(side1,side2,side3)):
    print("valid triangle")
else:
    print("not valid triangle")