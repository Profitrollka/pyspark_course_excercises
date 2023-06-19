# функция разбивки тегов
def split_tags(tags):
    if tags:
        return tags.split('|')
    else:
        return []