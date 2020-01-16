from talon.signature.bruteforce import extract_signature

# test Message_ID:"<17954197.1075855688641.JavaMail.evans@thyme>"
def find_my_signature(message):
    # extract text before -----Original Message-----
    if message.__contains__("-----Original Message-----"):
        message = message.split("-----Original Message-----", 1)[1]
    # extract signature
    text, signature = extract_signature(message)
    return signature


if __name__ == "__main__":
    message = """Hello, this is my first message
    --
    Thank you,
    Bob Smith
    '-----Original Message-----
    Hi, this is the original message.
    --
    All the best,
    Joe"""

    result = find_my_signature(message)
    print(result)