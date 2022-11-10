import os

import matplotlib.pyplot as plt
import cv2


def draw_bbox(img_path: str, x0: float, x1: float, y0: float, y1: float):
    img = cv2.imread(img_path)[:,:,::-1]
    # Create figure and axes
    fig, ax = plt.subplots()

    # Display the image
    ax.imshow(img)

    # Coordinates of rectangle vertices
    # in clockwise order
    xs = [x0, x1, x1, x0, x0]
    ys = [y0, y0, y1, y1, y0]
    ax.plot(xs, ys, color="red")

    plt.show()