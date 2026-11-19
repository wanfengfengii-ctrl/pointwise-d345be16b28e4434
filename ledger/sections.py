from collections.abc import Iterable


KNOWN_SECTIONS = ('east', 'west')


def require_identifier(value: str, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f'{label} must be a nonempty string')
    try:
        value.encode('utf-8')
    except UnicodeEncodeError as error:
        raise ValueError(f'{label} must be valid UTF-8 text') from error
    return value


def require_section(section: str) -> str:
    if not isinstance(section, str) or section not in KNOWN_SECTIONS:
        raise ValueError(f'unknown section: {section!r}')
    return section


def normalize_scope(sections: Iterable[str]) -> tuple[str, ...]:
    if isinstance(sections, (str, bytes)):
        raise ValueError('sections must be a collection of section identifiers')
    try:
        return tuple(sorted({require_section(section) for section in sections}))
    except TypeError as error:
        raise ValueError('sections must be a collection of section identifiers') from error
