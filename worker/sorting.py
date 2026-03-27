def quicksort(values: list[int]) -> list[int]:
    stack: list[tuple[int, int]] = [(0, len(values) - 1)]
    while stack:
        low, high = stack.pop()
        if low >= high:
            continue
        pivot = values[high]
        i = low
        for j in range(low, high):
            if values[j] <= pivot:
                values[i], values[j] = values[j], values[i]
                i += 1
        values[i], values[high] = values[high], values[i]
        if i - 1 > low:
            stack.append((low, i - 1))
        if i + 1 < high:
            stack.append((i + 1, high))
    return values
