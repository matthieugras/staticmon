include(FetchContent)

FetchContent_Declare(lexy
    GIT_REPOSITORY https://github.com/foonathan/lexy.git
    GIT_TAG af01ecd0aa8b774b9de62e6d12b9d69a1060ad20)

FetchContent_MakeAvailable(lexy)
