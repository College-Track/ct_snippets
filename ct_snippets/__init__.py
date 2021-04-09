# def copy_style():

#     import os
#     import matplotlib

#     from pkg_resources import resource_string

#     files = [
#         "college_track.mplstyle",
#     ]

#     for fname in files:
#         path = os.path.join(matplotlib.get_configdir(), fname)
#         text = resource_string(__name__, fname).decode()
#         open(path, "w").write(text)

