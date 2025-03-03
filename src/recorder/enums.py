from pytgcalls.types import Device as CallsDevice
from enum import Enum

import typing


T = typing.TypeVar("T")


class FrameDeviceEnum(Enum):
    # NOTE: format: DEVICE = (EXTENSION, MIME_TYPE)
    MICROPHONE = ("ogg", "audio/ogg")
    CAMERA = ("h264", "video/h264")
    # SCREEN = ("mp4", "video/mp4")


CALLS_DEVICE_TO_FRAME_DEVICE = {
    CallsDevice.MICROPHONE: FrameDeviceEnum.MICROPHONE,
    CallsDevice.SPEAKER: FrameDeviceEnum.MICROPHONE,
    # CallsDevice.SCREEN: FrameDeviceEnum.SCREEN,  # TODO: screen sharing
    CallsDevice.CAMERA: FrameDeviceEnum.CAMERA,
}


@typing.overload
def generate_devices_enum_dict(
    default: T,
    default_factory: None = ...
) -> dict[FrameDeviceEnum, T]: ...

@typing.overload
def generate_devices_enum_dict(
    default: None,
    default_factory: typing.Callable[[], T]
) -> dict[FrameDeviceEnum, T]: ...

def generate_devices_enum_dict(
    default: T | None = None,
    default_factory: typing.Callable[[], T] | None = None
) -> dict[FrameDeviceEnum, T]:
    if (default is not None and default_factory is not None) or (default is None and default_factory is None):
        raise ValueError("Either default or default_factory must be provided")

    if default_factory is not None:
        default = default_factory()

    return {
        device: typing.cast(T, default)
        for device in FrameDeviceEnum
    }
