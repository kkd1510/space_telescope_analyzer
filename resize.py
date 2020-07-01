import cv2
import glob
import os


def get_image_id(image_path):
    return os.path.basename(image_path).replace('.jpg', '').strip()


def image_resize(image, height=None, inter=cv2.INTER_AREA):
    dim = None
    (h, w) = image.shape[:2]
    r = height / float(h)
    dim = (int(w * r), height)
    resized = cv2.resize(image, dim, interpolation=inter)
    return resized


if __name__ == '__main__':

    images = glob.glob("images/*.jpg")

    for index, image in enumerate(images):
        image_id = get_image_id(image)
        print(index, "->", image_id)
        resized_path = f"resized/{image_id}_r.jpg"
        if not os.path.exists(resized_path):
            src = cv2.imread(image, cv2.IMREAD_UNCHANGED)
            if src is not None:
                resized = image_resize(src, 500)
                cv2.imwrite(resized_path, resized)
            else:
                print(f"{image_id} could not be read.")
