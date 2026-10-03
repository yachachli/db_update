"""Package the complete frozen POU release without loading serialized models."""
import hashlib
import json
from pathlib import Path
import zipfile


def write_model_bundle(source, archive):
    source, archive = Path(source), Path(archive)
    manifest = json.loads((source / 'manifest.json').read_text())
    names = ['manifest.json']
    for entry in manifest['categories'].values():
        names.extend(entry[k] for k in ('artifact', 'calibration', 'participation_artifact'))
    for name in names:
        path = source / name
        if path.resolve().parent != source.resolve():
            raise ValueError('Unsafe model artifact path')
        if not path.is_file():
            raise ValueError('Model bundle is incomplete')
    # Check both kinds of estimator before opening or replacing an archive.
    for entry in manifest['categories'].values():
        for field, checksum in (('artifact', 'sha256'), ('participation_artifact', 'participation_sha256')):
            if hashlib.sha256((source / entry[field]).read_bytes()).hexdigest() != entry[checksum]:
                raise ValueError('Model artifact integrity check failed')
    archive.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(archive, 'w', zipfile.ZIP_DEFLATED) as zipped:
        for name in sorted(set(names)):
            zipped.write(source / name, 'models/saved/pou_v1/' + name)
    digest = hashlib.sha256(archive.read_bytes()).hexdigest()
    archive.with_name(archive.name + '.sha256').write_text(digest + '  ' + archive.name + '\n')
    return archive
