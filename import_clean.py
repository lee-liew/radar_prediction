from scipy import misc
import numpy as np
import cv2
import os

# OS agnostic relative file path
# get the current directory path
dir = os.path.dirname(__file__)

# OS agnostic relative file path
# load sample image
test_image_path = os.path.join(os.sep, dir, 'sample_data', 'IDR423.T.201801310342.png')

# OS agnostic relative file path
# load colour to mm/hr concurrency table
rainfall_colour_table = os.path.join(os.sep, dir, 'sample_data', 'IDR423.T.201801310342.png')


im = cv2.imread(test_image_path)
key = np.genfromtxt(r"D:\cxwhite\Pycharm\playground\sample data\radar colours.csv", delimiter=',', dtype=int)
print(key)
unique_data = set(tuple(v) for m2d in im for v in m2d)

# for test1 in unique_data:
#     test = np.zeros((512, 512, 3), np.uint8)
#     test[:] = test1
#     cv2.imshow('image', test)
#     print(test1)
#     cv2.waitKey(1000)
#     cv2.destroyAllWindows()

indices = np.where(np.all(im == (192, 192, 192), axis=-1))

x = np.random.randn(20, 3)
print(x)
print()
x_new = x[np.sum(x, axis=1) > .5]
print(x_new)

test = 1
