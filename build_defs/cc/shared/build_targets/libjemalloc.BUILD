load("@rules_foreign_cc//foreign_cc:defs.bzl", "configure_make")

package(default_visibility = ["//visibility:public"])

filegroup(
    name = "jemalloc_srcs",
    srcs = glob(["jemalloc-5.3.0/**"]),
)

# https://bazel-contrib.github.io/rules_foreign_cc/configure_make.html
configure_make(
    name = "libjemalloc",
    autogen = True,
    configure_in_place = True,
    configure_options = [
        # Setting --enable-prof enables jemalloc memory profiling
        # "--enable-prof",
    ],
    copts = ["-Wno-int-conversion"],
    lib_source = ":jemalloc_srcs",
)

cc_library(
    name = "libjemalloc_static",
    linkopts = [
        "-lm",
        "-lstdc++",
        "-pthread",
    ],
    linkstatic = 1,
    deps = ["@jemalloc//:libjemalloc"],
    alwayslink = 1,
)
