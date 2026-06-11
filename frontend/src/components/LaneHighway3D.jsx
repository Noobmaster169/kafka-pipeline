import { useRef, useState, useMemo, Suspense } from "react";
import { Canvas, useFrame } from "@react-three/fiber";
import { useGLTF, Text, OrbitControls, Billboard } from "@react-three/drei";
import * as THREE from "three";

const SCALE = 10;           // world units per km along the road (z axis)
const LANE_X = [-1.6, 1.6];
const MODELS = ["sedan", "suv", "truck", "hatchback", "coupe"];
const MODEL_URL = (m) => `/models/${m}.glb`;

const hash = (s) => [...(s || "X")].reduce((n, c) => (n * 31 + c.charCodeAt(0)) >>> 0, 7);

/* ---------- infrastructure ---------- */
function Gantry({ z, label, limit, flashesRef, camId }) {
  const lens = useRef();
  useFrame(() => {
    if (!lens.current) return;
    const hot = performance.now() < (flashesRef.current[camId] || 0);
    lens.current.emissiveIntensity = hot ? 6 : 0.5;
    lens.current.emissive.set(hot ? "#ffffff" : "#4ade80");
  });
  return (
    <group position={[0, 0, z]}>
      {[-5, 5].map((px) => (
        <mesh key={px} position={[px, 2.6, 0]}>
          <boxGeometry args={[0.5, 5.2, 0.5]} />
          <meshStandardMaterial color="#1f2229" />
        </mesh>
      ))}
      <mesh position={[0, 5.2, 0]}>
        <boxGeometry args={[10.5, 0.7, 0.5]} />
        <meshStandardMaterial color="#1f2229" />
      </mesh>
      <mesh position={[0, 4.6, 0.35]}>
        <boxGeometry args={[1.1, 0.55, 0.4]} />
        <meshStandardMaterial color="#23272f" emissive="#4ade80" emissiveIntensity={0.5} ref={lens} />
      </mesh>
      <Billboard position={[0, 6.3, 0]}>
        <Text fontSize={0.7} color="#9aa3af" anchorX="center">
          {label} · {limit}
        </Text>
      </Billboard>
    </group>
  );
}

function Road({ length }) {
  return (
    <group>
      <mesh rotation={[-Math.PI / 2, 0, 0]} position={[0, 0, length / 2 - 6]}>
        <planeGeometry args={[14, length + 24]} />
        <meshStandardMaterial color="#0b0c10" />
      </mesh>
      {Array.from({ length: Math.ceil(length / 3) + 6 }).map((_, i) => (
        <mesh key={i} rotation={[-Math.PI / 2, 0, 0]} position={[0, 0.01, -6 + i * 3]}>
          <planeGeometry args={[0.18, 1.6]} />
          <meshBasicMaterial color="#2a2f3a" />
        </mesh>
      ))}
    </group>
  );
}

/* ---------- vehicles ---------- */
function Car({ data }) {
  const group = useRef();
  const { scene } = useGLTF(MODEL_URL(data.model));
  const body = useMemo(() => {
    const c = scene.clone(true);
    const tint = new THREE.Color(`hsl(${hash(data.plate) % 360}, 60%, 52%)`);
    c.traverse((o) => {
      if (!o.isMesh) return;
      o.material = o.material.clone();
      const n = `${o.material.name} ${o.name}`.toLowerCase();
      o.material.color = n.includes("wheel") || n.includes("tire")
        ? new THREE.Color("#15151a") : tint;
      if (data.overLimit) {
        o.material.emissive = new THREE.Color("#f43f5e");
        o.material.emissiveIntensity = 0.5;
      }
    });
    return c;
  }, [scene, data.plate, data.overLimit]);

  useFrame((_, dt) => {
    data.z += data.v * dt;
    if (group.current) group.current.position.set(data.x, 0, data.z);
  });

  return (
    <group ref={group}>
      <primitive object={body} />
      <Billboard position={[0, 1.6, 0]}>
        <Text fontSize={0.34} color={data.overLimit ? "#f43f5e" : "#cfd6df"} anchorX="center">
          {data.plate}
        </Text>
      </Billboard>
    </group>
  );
}

/* ---------- conductor ---------- */
function Traffic({ eventQueueRef, cameras, flashesRef }) {
  const cars = useRef(new Map());
  const [, bump] = useState(0);
  const endZ = (cameras.at(-1)?.position_km ?? 3) * SCALE + 14;
  const camByIdx = useMemo(
    () => Object.fromEntries(cameras.map((c) => [c.camera_id, c])), [cameras]);

  useFrame(() => {
    let changed = false;
    const q = eventQueueRef.current;
    if (q.length > 90) q.splice(0, q.length - 90);
    while (q.length) {
      const ev = q.shift();
      const cam = camByIdx[ev.camera_id];
      if (!cam) continue;
      const gz = (ev.position_km ?? cam.position_km) * SCALE;
      const limit = ev.speed_limit ?? cam.speed_limit;
      const over = ev.speed_reading > limit;
      if (over) flashesRef.current[cam.camera_id] = performance.now() + 450;

      const h = hash(ev.car_plate);
      const existing = cars.current.get(ev.car_plate);
      if (existing) {
        existing.v = Math.max(2, (ev.speed_reading / 100) * 7);
        existing.overLimit = existing.overLimit || over;
        if (existing.z < gz - 1) existing.z = gz - 1.2;
      } else {
        if (cars.current.size >= 30) {
          // evict the oldest car to make room
          const oldest = cars.current.keys().next().value;
          cars.current.delete(oldest);
        }
        cars.current.set(ev.car_plate, {
          plate: ev.car_plate,
          model: MODELS[h % MODELS.length],
          // jittered lane + stagger so simultaneous spawns don't stack
          x: LANE_X[h % LANE_X.length] + (((h >> 3) % 5) - 2) * 0.28,
          z: gz - 2.5 - ((h >> 6) % 45) / 10,
          v: Math.max(2, (ev.speed_reading / 100) * 7),
          overLimit: over,
        });
        changed = true;
      }
    }
    for (const [plate, c] of cars.current) {
      if (c.z > endZ) { cars.current.delete(plate); changed = true; }
    }
    if (changed) bump((n) => n + 1);
  });

  return <>{[...cars.current.values()].map((c) => <Car key={c.plate} data={c} />)}</>;
}

/* ---------- main export ---------- */
export default function LaneHighway3D({ cameras = [], eventQueueRef }) {
  const flashesRef = useRef({});
  const length = ((cameras.at(-1)?.position_km ?? 3) * SCALE) + 16;
  const mid = length / 2;

  return (
    <div style={{ height: 380 }}>
      <Canvas camera={{ position: [24, 9, mid], fov: 40 }} dpr={[1, 2]}>
        <ambientLight intensity={0.4} />
        <directionalLight position={[14, 16, mid]} intensity={1.2} />
        <fog attach="fog" args={["#0b0c10", 30, 110]} />
        <Suspense fallback={null}>
          <Road length={length} />
          {cameras.map((c) => (
            <Gantry key={c.camera_id} z={c.position_km * SCALE} camId={c.camera_id}
                    label={`CAM ${c.camera_id}`} limit={c.speed_limit} flashesRef={flashesRef} />
          ))}
          <Traffic eventQueueRef={eventQueueRef} cameras={cameras} flashesRef={flashesRef} />
        </Suspense>
        <OrbitControls enablePan={false} target={[0, 1, mid]}
                       minPolarAngle={0.4} maxPolarAngle={1.45}
                       minDistance={10} maxDistance={60} />
      </Canvas>
    </div>
  );
}