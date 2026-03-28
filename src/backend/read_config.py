import configparser
from pathlib import Path


def main():
    config_path = Path.cwd() / "config/config.ini"
    config = configparser.ConfigParser()
    config.read(config_path)

    return config


if __name__ == "__main__":
    main()
