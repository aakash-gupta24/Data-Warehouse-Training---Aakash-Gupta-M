original = {"a": 1, "b": 2}

inverted = {value: key for key, value in original.items()}

print(f"original dictionary:{original}")
print(f"inverted dictionary:{inverted}")
