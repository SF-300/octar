# Project Overview
Can be found in [README.md](./README.md)

# How-Tos
* Python tests can be run using `cd octar && uv run pytest`

# Design Principles
- KISS principle MUST be followed
- Dependency Inversion Principle (DIP) MUST be followed
- Bounded Contexts from Domain-Driven Design (DDD) MUST be the main organizational unit to structure the solutions space

# Other important stuff
- During refactoring NEVER drop comments - especially those that are prefixed with "NOTE:", "FIXME:", "TODO:", etc. They are there for a reason and contain critical information.