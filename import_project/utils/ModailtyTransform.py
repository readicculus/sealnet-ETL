import numpy as np
import cv2
class TransformMode:
    IRTOEO = 1
    EOTOIR = 2

class ModalityTransform():
    def __init__(self, H, mode):
        self.h = H
        self.mode = mode

    def transform_ir_to_eo(self, im_ir, im_eo):
        H = None
        if self.mode == TransformMode.IRTOEO:
            H = self.h
        elif self.mode == TransformMode.EOTOIR:
            H = np.linalg.inv(self.h)
        else:
            raise ("invalid mode")

        h, w, _ = im_eo.shape
        im_ir_3c = cv2.merge([im_ir, im_ir, im_ir])
        im_ir_3c[im_ir_3c == 0.] = 0.01
        im_proj = cv2.warpPerspective(im_ir_3c, H, (w, h))

        print(im_ir_3c.min())
        b_channel, g_channel, a_channel = cv2.split(im_proj)
        b_channel, g_channel, r_channel = cv2.split(im_eo)
        print(im_proj.min())

        # missing_mask = np.zeros(a_channel.shape)
        # missing_mask[a_channel == 0] = 1

        r_channel[a_channel == 0.] = 0.
        b_channel[a_channel == 0.] = 0.
        g_channel[a_channel == 0.] = 0.
        a_channel[a_channel == 0.] = 0.

        # r_channel[missing_mask_1c[:,1], missing_mask_1c[:,0]] = 255

        im_aligned = cv2.merge([b_channel, g_channel, r_channel, a_channel])
        return im_aligned  # , missing_mask

    def transform_eo_to_ir(self, im_eo, im_ir):
        H = None
        if self.mode == TransformMode.IRTOEO:
            H = np.linalg.inv(self.h)
        elif self.mode == TransformMode.EOTOIR:
            H = self.h
        else:
            raise ("invalid mode")

        h, w = im_ir.shape

        im_proj = cv2.warpPerspective(im_eo, H, (w, h))

        b_channel, g_channel, r_channel = cv2.split(im_proj)
        im_aligned = cv2.merge((b_channel, g_channel, r_channel, im_ir))
        return im_aligned
