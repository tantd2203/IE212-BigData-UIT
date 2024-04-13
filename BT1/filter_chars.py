import re

def remove_special_chars(input_string):
    # Regex pattern để loại bỏ các ký tự emojism, dấu chấm và ký tự đặc biệt "@"
    pattern = r'[\U0001F300-\U0001F5FF\U0001F600-\U0001F64F\U0001F680-\U0001F6FF\U0001F700-\U0001F77F\U0001F780-\U0001F7FF\U0001F800-\U0001F8FF\U0001F900-\U0001F9FF\U0001FA00-\U0001FA6F\U0001FA70-\U0001FAFF\.@\d]'

    # Sử dụng hàm sub của re để thay thế các ký tự không thuộc loại đã chỉ định bằng một chuỗi rỗng
    filtered_string = re.sub(pattern, '', input_string)
    
    return filtered_string

# Sử dụng hàm:
input_string = "This is a test 😊. Th1s 1s @ t3st. 💩💩💩"
filtered_result = remove_special_chars(input_string)
print(filtered_result)
