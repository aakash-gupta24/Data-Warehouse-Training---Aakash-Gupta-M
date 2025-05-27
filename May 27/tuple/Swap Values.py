def swap_tuples(tuple1, tuple2):
    return tuple2, tuple1

a = (1, 2, 3)
b = (4, 5, 6)

print("tuple a before swap:", a)
print("tuple b before swap:", b)
print("")
a, b = swap_tuples(a, b)

print("tuple a after swap:", a)
print("tuple b after swap:", b)
