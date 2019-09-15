import emoji

def deEmoji(text):
    allchars = [str for str in text]
    emoji_list = [c for c in allchars if c in emoji.UNICODE_EMOJI]
    clean_text = ''.join([str for str in text if not any(i in str for i in emoji_list)])
    return clean_text