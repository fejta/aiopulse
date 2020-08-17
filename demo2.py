#!/usr/bin/env python3

import asyncio
import socket
import sys


class Command:
    kind = ''
    arg = ''

    def __init__(self, kind, arg=None):
        self.kind = kind
        if arg:
            self.arg = arg

    def __repr__(self):
        if self.arg:
            return 'Command(%r, %r)' % (self.kind, self.arg)
        return 'Command(%r)' % self.kind

    ROOM= 'ROOM'
    NAME= 'NAME'
    MAC_ADDRESS = 'MAC'
    SERIAL_NUMBER = 'SN'
    LIST = 'v'

    DOWN = 'c'
    JOG_DOWN = 'cA'
    UP = 'o'
    JOG_UP = 'oA'
    MOVE = 'm'
    ROTATION = 'b'
    STOP = 's'
    POSITION = 'r'
    VOLTAGE = 'pVc'

    ERROR = 'E'

    ARG_QUERY = '?'
    questions = [
            ERROR,
            MAC_ADDRESS,
            NAME,
            ROOM,
            SERIAL_NUMBER,
            LIST,
            ERROR,
    ]

class Hub:
    device = '000'
    host = None
    port = 1487
    motors = None
    updated = False
    mac = ''
    name = ''
    unknown = None

    def __init__(self, host, port=None):
        if not host:
            raise ValueError('missing host')
        self.host = host
        if port:
            self.port = port
        self.motors = {}

    def __repr__(self):
        return 'Hub(%r, %r)' % (self.host, self.port)

    def __str__(self):
        parts = ['%s:%d (%s) %s has %d devices:' % (self.host, self.port, self.mac, self.name, len(self.motors))]
        for m in self.motors.values():
            parts.append('  ' + str(m))
        return '\n'.join(parts)

    async def connect(self):
        # Connect to the hub
        self.reader, self.writer = await asyncio.open_connection(self.host, self.port)
        # We are connected, get data about the hub
        await self.write(self.request_mac())
        await self.write(self.request_name())
        # And see what is paired with it
        await self.write(self.request_motors())

    async def read_all(self):
        """Reads a packet forever."""
        while True:
            await self.read_one()

    async def read_one(self):
        """Reads one packet."""
        pkt = await self.next_packet()
        await self.update(pkt)

    async def write(self, pkt):
        """Writes a packet."""
        buf = pkt.encode()
        self.writer.write(buf)
        await self.writer.drain()

    async def next_packet(self):
        """Returns the next packet."""
        # All commands start with ! and end with ;
        buf = await self.reader.readuntil(b';')
        pkt = Packet.decode(buf)
        return pkt

    def update_hub(self, pkt):
        """Update state to reflect this hub metadata packet."""
        unknown = False
        for c in pkt.commands:
            if c.kind == Command.NAME:
                self.name = c.arg
            elif c.kind == Command.MAC_ADDRESS:
                self.mac = c.arg
            else:
                unknown = True
        if not unknown:
            return

        self.unknown = self.unknown or []
        self.unknown.append(c)

    async def update(self, pkt):
        """Update state to reflect this hub or device packet."""
        self.updated = True
        device = pkt.device
        if device == self.device:
            self.update_hub(pkt)
            return
        if device == 'BR1':  # The hub
            return
        if device == 'EUC':  # Error unknown choice
            return
        if device not in self.motors:  # First time we've seen this motor
            added = True  # Do more stuff after updating
            motor = Motor(device)
            self.motors[device] = motor
        else:
            added = False
            motor = self.motors[device]
        motor.update(pkt)
        if not added:
            return
        if motor.style == 'B': # It is a hub (although we should get here)
            return

        await self.write(motor.request_name())
        await self.write(motor.request_room())
        await self.write(motor.request_position())
        if motor.style == 'D':  # Maybe also C?
            await self.write(motor.request_voltage())

    @classmethod
    def request_motors(cls):
        """Returns a packet that returns version info for all paired motors."""
        return Packet(cls.device, Command(Command.LIST, Command.ARG_QUERY))

    @classmethod
    def request_name(cls):
        """Return a packet that requests the name of this hub."""
        return Packet(cls.device, Command(Command.NAME, Command.ARG_QUERY))

    @classmethod
    def request_mac(cls):
        """Return a packet that requests the mac address of this hub."""
        return Packet(cls.device, Command(Command.MAC_ADDRESS, Command.ARG_QUERY))


class Motor:
    device = None
    room = ''
    name = ''
    major = 0
    minor = 0
    moving = False
    position = 0
    rotation = 0
    style = ''
    voltage = 0

    styles = {
            'B': 'Base',
            'D': 'DC Roller',
            'A': 'AC Roller',
            'L': 'Light',
            'C': 'Curtain',
            'S': 'Socket',
    }

    unknown = None
    err = None

    def __init__(self, device):
        if not device:
            raise ValueError('empty device')
        self.device = device

    def __repr__(self):
        return 'Motor(%r)' % self.device

    def __str__(self):
        parts = [
                '%s, a %s.%s %s, is %s in %s at %d%%, rotated %d degrees' % (
                    self.device,
                    self.major,
                    self.minor,
                    self.styles.get(self.style, 'unknown'),
                    self.name,
                    self.room,
                    self.position,
                    self.rotation,
                ),
        ]

        if self.style == 'D' or self.voltage:
            parts.append(' with %.2fV battery' % self.voltage)
        return ''.join(parts)

    def request_name(self):
        """Returns a packet that requests the device's name."""
        return Packet(self.device, Command(Command.NAME, Command.ARG_QUERY))

    def request_room(self):
        """Returns a packet that requests the device's room."""
        return Packet(self.device, Command(Command.ROOM, Command.ARG_QUERY))

    def request_version(self):
        """Returns a packet that requests the device's type and version."""
        return Packet(self.device, Command(Command.LIST, Command.ARG_QUERY))

    def request_position(self):
        """Returns a packet requesting the device's current position and rotation."""
        return Packet(self.device, Command(Command.POSITION, Command.ARG_QUERY))

    def request_voltage(self):
        """Returns a packet requesting the device's current voltage."""
        return Packet(self.device, Command(Command.VOLTAGE, Command.ARG_QUERY))

    def move_down(self):
        """Returns a packet to move the device all the way down (limited)."""
        return Packet(self.device, Command(Command.DOWN))

    def jog_down(self):
        """Returns a packet to jog the device sightly down (limited)."""
        return Packet(self.device, Command(Command.JOG_DOWN))

    def jog_up(self):
        """Returns a packet to job the device slightly up (limited)."""
        return Packet(self.device, Command(Command.JOG_UP))

    def move_up(self):
        """Returns a packet to move the device all the way up (limited)."""
        return Packet(self.device, Command(Command.UP))

    def set_position(self, pct):
        """Returns a packet to set the device to a percentage of the limits."""
        p = int(pct)
        if 100 > p < 0:
            raise ValueError('not between 0 and 100', pct)
        return Packet(self.device, Command(Command.MOVE, '%03d' % p))

    def set_rotation(self, pct):
        """Returns a packet to set the device rotation to a percentage of the limits."""
        p = int(pct)
        if 100 > p < 0:
            raise ValueError('not between 0 and 100', pct)
        return Packet(self.device, Command(Command.ROTATION, '%03d' % p))

    def stop(self):
        """Returns a packet to stop any device movment."""
        return Packet(self.device, Command(Command.STOP))

    def update(self, packet):
        """Updates device state with info in packet (stores last 100 unknown/invalid packets)."""
        if packet.device != self.device:
            raise ValueError('wrong device', self.device, packet.device)
        unknown = False
        err = False
        for cmd in packet.commands:
            kind, arg = cmd.kind, cmd.arg
            if kind == Command.ROOM:
                self.room = arg
            elif kind == Command.NAME:
                self.name = arg
            elif kind == Command.LIST and len(arg) == 3:
                self.style, self.major, self.minor = arg
            elif kind == Command.POSITION:
                try:
                    self.position = int(arg)
                except ValueError:
                    err = True
            elif kind == Command.ROTATION:
                try:
                    self.rotation = int(arg)
                except ValueError:
                    err = True
            elif kind == Command.VOLTAGE:
                try:
                    self.voltage = float(arg)/100.
                except ValueError:
                    err = True
            else:
                unknown = True
        if unknown:
            print('unknown packet', packet)
            self.unknown = self.unknown and self.unknown[:100] or []
            self.unknown.append(packet)
        if err:
            print('error packet', packet)
            self.err = self.err and self.err[:100] or []
            self.err.append(packet)


class Packet:
    device = None
    strength = None

    def __init__(self, device, *commands, strength=None):
        self.device = device
        self.commands = commands
        self.strength=strength

    def encode(self):
        """Returns the byte representation of this packet."""
        if not self.device:
            raise AttributeError('device unset')
        parts = ['!', self.device]
        for c in self.commands:
            parts.extend([c.kind, str(c.arg)])
        if self.strength:
            parts.append(',R')
            parts.append(self.strength)
        parts.append(';')
        return ''.join(parts).encode('utf-8')

    @staticmethod
    def decode(buf):
        """Decodes the bytes into a !YYYxxx,Rrr packet."""
        s = buf.decode('utf-8')
        if not s:
            raise ValueError('empty')
        if len(s) < 5:
            raise ValueError('too short', s[:1024])
        if s[0] != '!':
            raise ValueError('must start with !', s[:1024])
        if s[-1] != ';':
            raise ValueError('must end with ;', s[-1024:])

        # convert !stuff; into stuff
        payload = s[1:-1]

        # convert data,R01 into [data, 01] or [data]
        pieces = payload.rsplit(',R',1)
        if len(pieces) == 2:
            strength = pieces[1]
        else:
            strength = None

        # the data part of data,R01
        payload = pieces[0]
        # separate XYZblah into (XYZ, blah)
        device, body = payload[:3], payload[3:]

        # The body is <chars><numbers> possibly repeated
        # where the chars represent the command
        # and numbers represent optional data.
        special = False
        for q in Command.questions:
            if body[:len(q)] == q:
                commands = [Command(q, body[len(q):])]
                break
        else:
            commands = []
            pieces = []
            kind = None
            first = True
            for i, ch in enumerate(body):
                if ch >= '0' and ch <= '9':
                    if first:
                        if not pieces:
                            raise ValueError('missing command at char', i, body[:1024])
                        kind = ''.join(pieces)
                        pieces = []
                        first = False
                    pieces.append(ch)
                else:
                    if not first:
                        arg = ''.join(pieces)
                        pieces = []
                        commands.append(Command(kind, arg))
                        kind = None
                        first = True
                    pieces.append(ch)

            if first:
                kind = ''.join(pieces)
                pieces = []
            if kind:
                arg = ''.join(pieces)
                commands.append(Command(kind,arg))
        if not commands:
            raise ValueError('zero commands', body[:1024])

        return Packet(device, *commands, strength=strength)

    def __repr__(self):
        return 'Packet(%r, *%r)' % (self.device, self.commands)


async def report_forever(*hubs, delay=1.):
    while True:
        for hub in hubs:
            if hub.updated:
                print(hub)
                hub.updated = False
            await asyncio.sleep(delay)


def ip_address():
    """Current LAN IP address."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(('8.8.8.8', 1))
    addr = s.getsockname()[0]
    s.close()
    return addr


def near(addr):
    """Returns a list of 254 addr/24 ip addresses."""
    front, _ = addr.rsplit('.', 1)
    return ['%s.%d' % (front, n) for n in range(1,255)]


async def scan(host, port):
    """Returns whether host:port has an open TCP server."""
    try:
        _, w = await asyncio.open_connection(host, port)
    except ConnectionRefusedError:
        return False
    w.close()
    await w.wait_closed()
    return True


class Scanner:
    """Scanner can concurrently scan for hosts listening on a port."""
    listening = None

    def __init__(self):
        self.listening = set()

    async def scan(self, host, port):
        """Adds the host:port combo to the listening set on success."""
        if await scan(host, port):
            self.listening.add((host, port))

    async def scanall(self, *hosts, port=1487):
        """Returns the set of (host, port) tuples that are listening."""
        self.listening.clear()
        await asyncio.gather(*(self.scan(h, port) for h in hosts))
        return self.listening


async def main():
    print('Determining ip address...', end='', flush=True)
    addr = ip_address()
    print(addr)
    addrs = near(addr)
    print('Scanning for nearby hubs...', end='', flush=True)
    s = Scanner()
    found = await s.scanall(*addrs)
    if not s.listening:
        print('no hubs found')
        sys.exit(1)

    hubs = [Hub(h, port=p) for (h, p) in found]
    print('found %d hubs:' % len(hubs))
    for h in hubs:
        print('  ',h)
    print('connecting...', end='', flush=True)
    await asyncio.gather(*(h.connect() for h in hubs))
    print('done')
    print('starting up for 60s...')
    await asyncio.wait({
        report_forever(*hubs),
        *(h.read_all() for h in hubs),
    }, timeout=60, return_when=asyncio.FIRST_EXCEPTION)

if __name__ == '__main__':
    asyncio.run(main())
