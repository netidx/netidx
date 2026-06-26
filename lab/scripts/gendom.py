import sys, xml.etree.ElementTree as ET

src, name, disk, mac, net = sys.argv[1:6]
tree = ET.parse(src)
root = tree.getroot()

root.find('name').text = name
for tag in ('uuid', 'metadata'):
    for e in root.findall(tag):
        root.remove(e)

dev = root.find('devices')
for d in list(dev.findall('disk')):
    if d.get('device') == 'cdrom':
        dev.remove(d)
        continue
    if d.get('device') == 'disk':
        s = d.find('source')
        s.set('file', '/var/lib/libvirt/images/' + disk)
        s.attrib.pop('index', None)
        for child in ('backingStore', 'alias'):
            c = d.find(child)
            if c is not None:
                d.remove(c)

for i in dev.findall('interface'):
    i.find('mac').set('address', mac)
    s = i.find('source')
    s.clear()
    s.set('network', net)
    for child in ('target', 'alias'):
        c = i.find(child)
        if c is not None:
            i.remove(c)

try:
    ET.indent(tree)
except Exception:
    pass
tree.write(sys.stdout, encoding='unicode')
