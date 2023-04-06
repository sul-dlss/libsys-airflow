# needed to import packages in the plugin

import pathlib
import sys

root_directory = pathlib.Path(__file__).parent
plugins_directory = root_directory / "plugins"

sys.path.append(str(plugins_directory))
