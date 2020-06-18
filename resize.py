import cv2


def image_resize(image, height=None, inter=cv2.INTER_AREA):
    dim = None
    (h, w) = image.shape[:2]
    r = height / float(h)
    dim = (int(w * r), height)
    resized = cv2.resize(image, dim, interpolation=inter)
    return resized

filename = 'heic2005a'
src = cv2.imread(f'{filename}.jpg', cv2.IMREAD_UNCHANGED)


cv2.imwrite(f"{filename}_r.jpg", image_resize(src, 1000))

