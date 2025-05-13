import os
import json
import time
import logging
from enum import Enum
import shutil
from datetime import datetime
import asyncio

class WALEntryStatus(Enum):
    """Estados posibles para una entrada del WAL"""
    PENDING = "PENDING"  # La operación está pendiente de procesamiento
    PROCESSING = "PROCESSING"  # La operación está en proceso
    COMPLETED = "COMPLETED"  # La operación ha sido completada exitosamente
    FAILED = "FAILED"  # La operación ha fallado

class WALEntry:
    """Representa una entrada en el Write Ahead Log"""
    def __init__(self, message_id, batch, content, status=WALEntryStatus.PENDING):
        self.id = message_id
        self.batch = batch
        self.content = content
        self.status = status
        self.created_at = datetime.now().isoformat()
        self.updated_at = self.created_at
    
    def to_dict(self):
        """Convierte la entrada a un diccionario para serialización"""
        return {
            "id": self.id,
            "batch": self.batch,
            "content": self.content,
            "status": self.status.value,
            "created_at": self.created_at,
            "updated_at": datetime.now().isoformat()
        }
    
    @classmethod
    def from_dict(cls, data):
        """Crea una instancia de WALEntry a partir de un diccionario"""
        entry = cls(
            message_id=data["id"],
            batch=data["batch"],
            content=data["content"],
            status=WALEntryStatus(data["status"])
        )
        entry.created_at = data["created_at"]
        entry.updated_at = data["updated_at"]
        return entry

class WriteAheadLog:
    """Implementación de Write Ahead Log para garantizar durabilidad de operaciones"""
    def __init__(self, log_dir, max_entries=10000, auto_cleanup=True):
        self.log_dir = log_dir
        self.current_log_file = None
        self.max_entries = max_entries
        self.auto_cleanup = auto_cleanup
        self._entries_count = 0
        
        # Crear directorio si no existe
        os.makedirs(self.log_dir, exist_ok=True)
        
        # Inicializar el archivo de log
        self._init_log_file()
        
        logging.info(f"WAL inicializado en {self.log_dir}")
    
    def _init_log_file(self):
        """Inicializa o rota el archivo de log si es necesario"""
        timestamp = int(time.time())
        self.current_log_file = os.path.join(self.log_dir, f"wal_{timestamp}.log")
        
        # Si existe un archivo anterior, verificar si necesita rotación
        if os.path.exists(self.current_log_file):
            with open(self.current_log_file, 'r') as f:
                self._entries_count = sum(1 for _ in f)
            
            if self._entries_count >= self.max_entries:
                self._rotate_log()
        
        logging.info(f"Usando archivo WAL: {self.current_log_file}")
    
    def _rotate_log(self):
        """Rota el archivo de log cuando alcanza el límite de entradas"""
        old_file = self.current_log_file
        timestamp = int(time.time())
        new_file = os.path.join(self.log_dir, f"wal_{timestamp}.log")
        
        # Verificar si hay entradas pendientes antes de rotar
        pending_entries = self._get_entries_by_status([WALEntryStatus.PENDING, WALEntryStatus.PROCESSING])
        
        if pending_entries:
            # Copiar solo las entradas pendientes al nuevo archivo
            with open(new_file, 'w') as f_new:
                for entry in pending_entries:
                    f_new.write(json.dumps(entry.to_dict()) + '\n')
        else:
            # Crear un archivo vacío
            open(new_file, 'w').close()
        
        # Actualizar el archivo actual y contador de entradas
        self.current_log_file = new_file
        self._entries_count = len(pending_entries)
        
        # Opcional: archivar el antiguo o eliminarlo si auto_cleanup está activado
        if self.auto_cleanup:
            archive_dir = os.path.join(self.log_dir, "archive")
            os.makedirs(archive_dir, exist_ok=True)
            archived_file = os.path.join(archive_dir, os.path.basename(old_file))
            shutil.move(old_file, archived_file)
            logging.info(f"WAL rotado: {old_file} -> {archived_file}")
        else:
            logging.info(f"WAL rotado: {old_file} -> {new_file}")
    
    async def append(self, message_id, message_data):
        """Agrega una nueva entrada al WAL"""
        entry = WALEntry(
            message_id=message_id,
            batch=message_data.get('batch'),
            content=message_data.get('content')
        )
        
        # Escribir entrada al log (operación de E/S, usar run_in_executor)
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._append_sync, entry)
        
        return entry
    
    def _append_sync(self, entry):
        """Operación sincrónica para agregar entrada al WAL"""
        with open(self.current_log_file, 'a') as f:
            f.write(json.dumps(entry.to_dict()) + '\n')
        
        self._entries_count += 1
        if self._entries_count >= self.max_entries:
            self._rotate_log()
    
    async def update_status(self, message_id, new_status):
        """Actualiza el estado de una entrada en el WAL"""
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._update_status_sync, message_id, new_status)
    
    def _update_status_sync(self, message_id, new_status):
        """Operación sincrónica para actualizar el estado de una entrada"""
        entries = []
        updated = False
        
        # Leer todas las entradas
        if os.path.exists(self.current_log_file):
            with open(self.current_log_file, 'r') as f:
                for line in f:
                    entry_dict = json.loads(line.strip())
                    
                    # Actualizar la entrada que coincide con el ID
                    if entry_dict["id"] == message_id:
                        entry_dict["status"] = new_status.value
                        entry_dict["updated_at"] = datetime.now().isoformat()
                        updated = True
                    
                    entries.append(entry_dict)
        
        # Si se actualizó alguna entrada, reescribir el archivo
        if updated:
            with open(self.current_log_file, 'w') as f:
                for entry_dict in entries:
                    f.write(json.dumps(entry_dict) + '\n')
    
    def _get_entries_by_status(self, statuses):
        """Obtiene las entradas con los estados especificados"""
        entries = []
        
        if not os.path.exists(self.current_log_file):
            return entries
        
        with open(self.current_log_file, 'r') as f:
            for line in f:
                entry_dict = json.loads(line.strip())
                entry_status = WALEntryStatus(entry_dict["status"])
                if entry_status in statuses:
                    entries.append(WALEntry.from_dict(entry_dict))
        
        return entries
    
    async def get_pending_entries(self):
        """Obtiene todas las entradas pendientes de procesamiento"""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, 
            self._get_entries_by_status, [WALEntryStatus.PENDING, WALEntryStatus.PROCESSING])
    
    async def recover(self):
        """Recupera las entradas pendientes para su reprocesamiento"""
        return await self.get_pending_entries()
    
    async def cleanup_completed(self, retention_hours=24):
        """Limpia las entradas completadas más antiguas que retention_hours"""
        if not self.auto_cleanup:
            return
        
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._cleanup_completed_sync, retention_hours)
    
    def _cleanup_completed_sync(self, retention_hours):
        """Operación sincrónica para limpiar entradas completadas"""
        now = datetime.now()
        entries = []
        cleaned = 0
        
        if os.path.exists(self.current_log_file):
            with open(self.current_log_file, 'r') as f:
                for line in f:
                    entry_dict = json.loads(line.strip())
                    status = WALEntryStatus(entry_dict["status"])
                    
                    # Conservar entradas pendientes, procesando o fallidas
                    if status not in [WALEntryStatus.COMPLETED]:
                        entries.append(entry_dict)
                        continue
                    
                    # Comprobar antigüedad de entradas completadas
                    updated_at = datetime.fromisoformat(entry_dict["updated_at"])
                    age = (now - updated_at).total_seconds() / 3600
                    
                    if age < retention_hours:
                        entries.append(entry_dict)
                    else:
                        cleaned += 1
        
        # Reescribir el archivo sin las entradas eliminadas
        if cleaned > 0:
            with open(self.current_log_file, 'w') as f:
                for entry_dict in entries:
                    f.write(json.dumps(entry_dict) + '\n')
            
            self._entries_count = len(entries)
            logging.info(f"Limpieza WAL: {cleaned} entradas completadas eliminadas")