

class Helper:
    def __init__(self):
        pass

    def get_image_text(self, image_list, size):
        for image in image_list:
            if image['size'] == size:
                return image['#text']
        return None